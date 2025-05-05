import sys
from mpi4py import MPI
from dht_globals import *
from command import commandNode

# Global state variables
rank = None
num_processes = None
node_active = False
node_id = None 
parent_id = None
child_rank = None
kv_store = {}

def terminate_head_node():
    """shutdown all nodes."""
    signal = MPI.COMM_WORLD.recv(source=num_processes - 1, tag=END)
    for i in range(1, num_processes - 1):
        MPI.COMM_WORLD.send(signal, dest=i, tag=END)
    MPI.Finalize()
    sys.exit(0)

def terminate_storage_node():
    """Storage node shutdown."""
    _ = MPI.COMM_WORLD.recv(source=0, tag=END)
    MPI.Finalize()
    sys.exit(0)

def shutdown():
    if rank == 0:
        terminate_head_node()
    else:
        terminate_storage_node()

def initialize_new_node(source):
    """Initialize a new storage node inserted into the ring."""
    global node_active, parent_id, child_rank, node_id
    parent_rank, next_child_rank, parent_storage_id, new_node_id = MPI.COMM_WORLD.recv(source=source, tag=INIT_NODE)
    node_id = new_node_id
    parent_id = parent_storage_id
    child_rank = next_child_rank
    node_active = True

def update_parent_info(source):
    """Update the parent ID during node reconfiguration."""
    global parent_id
    parent_id = MPI.COMM_WORLD.recv(source=source, tag=UPDATE_PARENT)

def respond_to_storage_id_request(source):
    """Reply with this node's storage ID."""
    _ = MPI.COMM_WORLD.recv(source=source, tag=GET_STORAGE_ID)
    response = node_id if node_active else -1
    MPI.COMM_WORLD.send(response, dest=source, tag=GET_STORAGE_ID)

def migrate_keys_to_new_node(new_rank, new_id):
    """Send key-value pairs in this node's range to the new node."""
    keys_to_migrate = {k: v for k, v in kv_store.items() if parent_id < k <= new_id}
    for k in keys_to_migrate:
        del kv_store[k]
    MPI.COMM_WORLD.send(keys_to_migrate, dest=new_rank, tag=REDIST)

def receive_keys(source):
    """Receive redistributed keys and integrate into current store."""
    incoming_data = MPI.COMM_WORLD.recv(source=source, tag=REDIST)
    kv_store.update(incoming_data)

def process_node_removal(source):
    """Transfer data to parent and signal disconnection."""
    global node_active, node_id, parent_id, child_rank
    parent_rank = MPI.COMM_WORLD.recv(source=source, tag=REMOVE_NODE)
    
    for k, v in kv_store.items():
        MPI.COMM_WORLD.send((k, v), dest=parent_rank, tag=PUT)
    
    kv_store.clear()
    MPI.COMM_WORLD.send(child_rank, dest=parent_rank, tag=NEW_CHILD_RANK)

    node_active = False
    node_id = None
    parent_id = None
    child_rank = None

def forward_new_node_info(new_rank, new_id):
    """Send current ring links to new node."""
    info = (rank, child_rank, node_id, new_id)
    MPI.COMM_WORLD.send(info, dest=new_rank, tag=INIT_NODE)

def patch_ring_after_add(new_rank, new_id):
    """Update this node's child and inform old child of new parent."""
    global child_rank
    MPI.COMM_WORLD.send(new_id, dest=child_rank, tag=UPDATE_PARENT)
    child_rank = new_rank

def add_node(source):
    """Handle new node joining the ring."""
    global child_rank
    new_rank, new_id = MPI.COMM_WORLD.recv(source=source, tag=ADD)

    if node_active and (rank == 0 or parent_id is not None):
        if node_id < new_id < query_storage_id(child_rank):
            forward_new_node_info(new_rank, new_id)
            patch_ring_after_add(new_rank, new_id)
            migrate_keys_to_new_node(new_rank, new_id)
            MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
        else:
            MPI.COMM_WORLD.send((new_rank, new_id), dest=child_rank, tag=ADD)
    else:
        MPI.COMM_WORLD.send((new_rank, new_id), dest=child_rank, tag=ADD)

def remove_node(source):
    """Handle node removal from ring."""
    global child_rank
    remove_id = MPI.COMM_WORLD.recv(source=source, tag=REMOVE)

    if node_active and query_storage_id(child_rank) == remove_id:
        MPI.COMM_WORLD.send(rank, dest=child_rank, tag=REMOVE_NODE)
        child_rank = MPI.COMM_WORLD.recv(source=child_rank, tag=NEW_CHILD_RANK)
        MPI.COMM_WORLD.send(node_id, dest=child_rank, tag=UPDATE_PARENT)
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
    else:
        MPI.COMM_WORLD.send(remove_id, dest=child_rank, tag=REMOVE)

def query_storage_id(remote_rank):
    """Request and receive a node’s storage ID."""
    MPI.COMM_WORLD.send(None, dest=remote_rank, tag=GET_STORAGE_ID)
    return MPI.COMM_WORLD.recv(source=remote_rank, tag=GET_STORAGE_ID)

def put_key_value(source):
    """Insert or route a key-value pair."""
    key, value = MPI.COMM_WORLD.recv(source=source, tag=PUT)

    if parent_id is not None and parent_id < key <= node_id:
        kv_store[key] = value
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)
    else:
        MPI.COMM_WORLD.send((key, value), dest=child_rank, tag=PUT)

def get_key_value(source):
    """Retrieve or forward a key request."""
    key = MPI.COMM_WORLD.recv(source=source, tag=GET)

    if parent_id is not None and parent_id < key <= node_id:
        value = kv_store.get(key, -1)
        MPI.COMM_WORLD.send((value, node_id), dest=0, tag=RETVAL)
    else:
        MPI.COMM_WORLD.send(key, dest=child_rank, tag=GET)

def forward_ack_to_client():
    if rank == 0:
        _ = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=ACK)
        MPI.COMM_WORLD.send(0, dest=num_processes - 1, tag=ACK)

def forward_retval_to_client():
    if rank == 0:
        result = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=RETVAL)
        MPI.COMM_WORLD.send(result, dest=num_processes - 1, tag=RETVAL)

def handleMessages():
    status = MPI.Status()
    while True:
        MPI.COMM_WORLD.probe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()

        if tag == END: shutdown()
        elif tag == ADD: add_node(source)
        elif tag == REMOVE: remove_node(source)
        elif tag == PUT: put_key_value(source)
        elif tag == GET: get_key_value(source)
        elif tag == ACK: forward_ack_to_client()
        elif tag == RETVAL: forward_retval_to_client()
        elif tag == GET_STORAGE_ID: respond_to_storage_id_request(source)
        elif tag == INIT_NODE: initialize_new_node(source)
        elif tag == UPDATE_PARENT: update_parent_info(source)
        elif tag == REDIST: receive_keys(source)
        elif tag == REMOVE_NODE: process_node_removal(source)
        else:
            print(f"[Rank {rank}] ERROR: Unexpected tag {tag}")
            sys.exit(1)

if __name__ == "__main__":
    num_processes = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()

    if rank == 0:
        node_id = 0
        child_rank = num_processes - 2
        node_active = True

    elif rank == num_processes - 2:
        node_id = MAX
        parent_id = 0
        child_rank = 0
        node_active = True

    if rank < num_processes - 1:
        handleMessages()
    else:
        commandNode()
