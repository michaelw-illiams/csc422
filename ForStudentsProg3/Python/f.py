import sys  # Provides access to system-specific parameters and functions
from mpi4py import MPI  # Imports the MPI module for parallel processing
from dht_globals import *  # Imports shared constants like message tags (e.g., PUT, GET, etc.)
from command import commandNode  # Imports the function that handles the command interface (used by rank N-1)

# Global state variables shared across all ranks
rank = None  # This process's MPI rank, assigned during initialization
num_processes = None  # Total number of MPI processes started via mpirun
node_active = False  # Boolean indicating whether this node is active in the DHT ring
node_id = None  # Logical ID used for key range partitioning in the DHT (1-1000)
parent_id = None  # ID of this node's logical parent in the DHT ring
child_rank = None  # Rank of this node's child in the ring (used for forwarding messages)
kv_store = {}  # Dictionary that stores key-value pairs assigned to this node

# --- Termination Routines ---

def terminate():
    """
    Purpose: Handles clean shutdown of the entire system when the END command is issued.
    This function is called only by the head node (rank 0), since it is the node that receives
    the END command from the command node. It propagates the END message to all other storage
    nodes and then finalizes MPI for a clean exit. This design ensures all nodes shut down in
    a coordinated and deterministic way.
    """
    signal = MPI.COMM_WORLD.recv(source=num_processes - 1, tag=END)  # Receive END signal from command node
    for i in range(1, num_processes - 1):  # Loop through all storage node ranks (excluding command node)
        MPI.COMM_WORLD.send(signal, dest=i, tag=END)  # Forward END signal to each node
    MPI.Finalize()  # Finalize MPI runtime (required before exiting)
    sys.exit(0)  # Exit the head node process

def kill_node():
    """
    Purpose: Ensures proper shutdown of a storage node after receiving an END message.
    This is a helper function called by any non-head node when it is time to shut down.
    It matches the END message from rank 0, calls MPI finalization, and exits cleanly.
    """
    _ = MPI.COMM_WORLD.recv(source=0, tag=END)  # Wait to receive END from head node
    MPI.Finalize()  # Finalize MPI to release resources
    sys.exit(0)  # Exit the process

def shutdown():
    """
    Purpose: Unified shutdown dispatcher that determines whether this node should
    shut down all nodes (head) or only itself (non-head).
    This abstraction makes the shutdown logic clearer and avoids repetition.
    """
    if rank == 0:  # If this is the head node
        terminate()  # Call the terminate routine
    else:
        kill_node()  # Otherwise just shut down this node

# --- Node Initialization and Reconfiguration ---

def init_node(source):
    """
    Purpose: Initializes a newly added node into the DHT ring with appropriate parent,
    child, and ID assignments. This is necessary to maintain the correct DHT ring topology.
    This method is only called once per rank when it is being added (via ADD command) and
    ensures that the node is configured to participate in data storage and routing.
    """
    global node_active, parent_id, child_rank, node_id  # Declare globals to modify their values
    parent_rank, next_child_rank, parent_storage_id, new_node_id = MPI.COMM_WORLD.recv(source=source, tag=INIT_NODE)  # Receive initialization data from parent node
    node_id = new_node_id  # Set this node's storage ID
    parent_id = parent_storage_id  # Set the storage ID of this node's parent
    child_rank = next_child_rank  # Set the MPI rank of this node's child
    node_active = True  # Mark this node as active

def update(source):
    """
    Purpose: Updates this node's parent storage ID during ring reconfiguration (e.g., after ADD/REMOVE).
    This is necessary to maintain correct routing of keys and commands as the topology changes.
    Called when a node is informed of a new parent by its former parent or child.
    """
    global parent_id  # Declare global so we can update it
    parent_id = MPI.COMM_WORLD.recv(source=source, tag=UPDATE_PARENT)  # Receive and set new parent ID

def respond(source):
    """
    Purpose: Handles a GET_STORAGE_ID query from another node.
    This is used by nodes when trying to determine where a new node should be placed
    or which node is being targeted for removal. It provides a way to inspect a node’s logical ID
    in a ring structure where direct access is not allowed (nodes only talk to parent/child).

    Design rationale: Uses a simple request-response model. The caller sends a message with
    tag=GET_STORAGE_ID, and this method replies with the logical ID, or -1 if inactive.
    """
    _ = MPI.COMM_WORLD.recv(source=source, tag=GET_STORAGE_ID)  # Receive the request (no payload needed)
    response = node_id if node_active else -1  # If the node is active, return its ID; otherwise return -1
    MPI.COMM_WORLD.send(response, dest=source, tag=GET_STORAGE_ID)  # Send the ID (or -1) back to the requester

# --- Key Redistribution ---

def move(new_rank, new_id):
    """
    Purpose: Sends the portion of key-value pairs that the new node is now responsible for.
    Used during the ADD operation, when a new node splits a key range.
    The range is determined using the parent_id and new_id of the new node.
    The current node filters its local kv_store for keys in that range, sends them to the new node,
    and deletes them from its store.
    """
    keys_to_migrate = {k: v for k, v in kv_store.items() if parent_id < k <= new_id}  # Collect all keys this new node should take over
    for k in keys_to_migrate:  # Loop through the keys to be moved
        del kv_store[k]  # Remove them from this node's store
    MPI.COMM_WORLD.send(keys_to_migrate, dest=new_rank, tag=REDIST)  # Send key-value pairs to new node

def receive_keys(source):
    """
    Purpose: Receives redistributed key-value pairs during ring changes (ADD/REMOVE).
    When a node takes ownership of a key range, it receives all the keys in that range
    via a single REDIST message and updates its local kv_store.
    """
    incoming_data = MPI.COMM_WORLD.recv(source=source, tag=REDIST)  # Receive dictionary of key-value pairs
    kv_store.update(incoming_data)  # Merge received keys into this node's kv_store

# --- Node Removal ---

def remove_help(source):
    """
    Purpose: Performs cleanup for a node that is being removed from the DHT.
    It sends all of its data to its parent node, informs the parent of its child,
    clears its state, and deactivates itself. Called by a node upon receiving REMOVE_NODE.
    """
    global node_active, node_id, parent_id, child_rank  # Globals modified due to node deactivation
    parent_rank = MPI.COMM_WORLD.recv(source=source, tag=REMOVE_NODE)  # Receive the rank of the parent node

    for k, v in kv_store.items():  # Send each key-value pair to the parent node
        MPI.COMM_WORLD.send((k, v), dest=parent_rank, tag=PUT)

    kv_store.clear()  # Clear local store after data is transferred
    MPI.COMM_WORLD.send(child_rank, dest=parent_rank, tag=NEW_CHILD_RANK)  # Notify parent of new child

    node_active = False  # Mark this node as inactive
    node_id = None  # Reset storage ID
    parent_id = None  # Clear parent info
    child_rank = None  # Clear child info

# --- Forwarding Ring Metadata ---

def forward(new_rank, new_id):
    """
    Purpose: Sends all necessary information to the new node being added to the ring.
    Includes this node's rank (as parent), its child rank, its storage ID, and the new node's ID.
    This sets up the new node's view of the ring correctly.
    """
    info = (rank, child_rank, node_id, new_id)  # Pack metadata needed for initialization
    MPI.COMM_WORLD.send(info, dest=new_rank, tag=INIT_NODE)  # Send it to the new node

def add_help(new_rank, new_id):
    """
    Purpose: Updates this node's child to point to the new node, and informs the
    old child of the new parent. This maintains the ring structure during an ADD operation.
    """
    global child_rank  # Will update child_rank to point to the new node
    MPI.COMM_WORLD.send(new_id, dest=child_rank, tag=UPDATE_PARENT)  # Inform old child of new parent
    child_rank = new_rank  # Update this node’s child pointer to the new node

# --- ADD and REMOVE Handling ---

def add_node(source):
    """
    Purpose: Handles insertion of a new node into the DHT ring.
    Routes the ADD command through the ring to locate the correct parent node for the new node.
    Once the parent is identified, the parent calls `forward()` to send initialization data,
    `add_help()` to update the ring structure, and `move()` to transfer relevant key-value pairs.
    ACK is only sent after all steps are completed to ensure consistency.
    """
    global child_rank  # This node might need to update its child
    new_rank, new_id = MPI.COMM_WORLD.recv(source=source, tag=ADD)  # Receive new node's rank and logical ID

    if node_active and (rank == 0 or parent_id is not None):  # Ensure this node is part of the ring
        if node_id < new_id < query(child_rank):  # Determine if the new ID fits between this node and its child
            forward(new_rank, new_id)  # Send initialization info to the new node
            add_help(new_rank, new_id)  # Update this node’s child and inform the old child
            move(new_rank, new_id)  # Migrate keys in the new node's range
            MPI.COMM_WORLD.send(0, dest=0, tag=ACK)  # Notify head node that operation completed
        else:
            MPI.COMM_WORLD.send((new_rank, new_id), dest=child_rank, tag=ADD)  # Forward ADD to child if not responsible
    else:
        MPI.COMM_WORLD.send((new_rank, new_id), dest=child_rank, tag=ADD)  # Forward blindly if inactive (edge case)

def remove_node(source):
    """
    Purpose: Handles the removal of a node from the DHT ring.
    Routes the REMOVE command until it reaches the parent of the node to be removed.
    The parent initiates the remove sequence by sending REMOVE_NODE to the child,
    waits for the new child rank, updates the ring, and confirms completion with an ACK.
    """
    global child_rank  # Will need to update child if we remove our current child
    remove_id = MPI.COMM_WORLD.recv(source=source, tag=REMOVE)  # Receive the ID of the node to remove

    if node_active and query(child_rank) == remove_id:  # If our child is the one to be removed
        MPI.COMM_WORLD.send(rank, dest=child_rank, tag=REMOVE_NODE)  # Send REMOVE_NODE signal to child
        child_rank = MPI.COMM_WORLD.recv(source=child_rank, tag=NEW_CHILD_RANK)  # Receive the new child rank
        MPI.COMM_WORLD.send(node_id, dest=child_rank, tag=UPDATE_PARENT)  # Notify new child of its parent
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)  # Send ACK to head after cleanup
    else:
        MPI.COMM_WORLD.send(remove_id, dest=child_rank, tag=REMOVE)  # Forward REMOVE if not our child

# --- Query, PUT, and GET Handling ---

def query(remote_rank):
    """
    Purpose: Sends a GET_STORAGE_ID query to another rank and returns its storage ID.
    Used primarily to determine where a new node should be inserted during an ADD,
    or which node should be removed.
    """
    MPI.COMM_WORLD.send(None, dest=remote_rank, tag=GET_STORAGE_ID)  # Ask for remote node’s ID
    return MPI.COMM_WORLD.recv(source=remote_rank, tag=GET_STORAGE_ID)  # Return the received ID

def put_key_value(source):
    """
    Purpose: Handles insertion of a (key, value) pair into the DHT.
    If this node owns the key (based on parent_id and node_id), it stores it.
    Otherwise, forwards the PUT command to its child. ACK is sent by the node that stores the value.
    """
    key, value = MPI.COMM_WORLD.recv(source=source, tag=PUT)  # Receive key-value pair to store

    if parent_id is not None and parent_id < key <= node_id:  # If this node owns the key range
        kv_store[key] = value  # Store the key-value pair locally
        MPI.COMM_WORLD.send(0, dest=0, tag=ACK)  # Notify head that the PUT is done
    else:
        MPI.COMM_WORLD.send((key, value), dest=child_rank, tag=PUT)  # Forward PUT to child

def get_key_value(source):
    """
    Purpose: Handles retrieval of a value for a given key.
    If this node owns the key, it retrieves it and returns the value and its ID.
    Otherwise, forwards the GET request to its child. Result is routed back via head node.
    """
    key = MPI.COMM_WORLD.recv(source=source, tag=GET)  # Receive the key to fetch

    if parent_id is not None and parent_id < key <= node_id:  # If this node owns the key
        value = kv_store.get(key, -1)  # Get value or return -1 if not found (should always exist in valid input)
        MPI.COMM_WORLD.send((value, node_id), dest=0, tag=RETVAL)  # Send result back to head
    else:
        MPI.COMM_WORLD.send(key, dest=child_rank, tag=GET)  # Forward GET to child

# --- Result Forwarding ---

def forward_ack_to_client():
    """
    Purpose: Called by the head node to relay an ACK back to the command node.
    This function is triggered when a PUT, ADD, or REMOVE operation completes.
    It waits for an ACK from any node in the ring and forwards it to the command node
    so that it knows the operation has finished.
    """
    if rank == 0:  # Only the head node performs this relay
        _ = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=ACK)  # Receive ACK from any storage node
        MPI.COMM_WORLD.send(0, dest=num_processes - 1, tag=ACK)  # Forward to the command node

def forward_retval_to_client():
    """
    Purpose: Called by the head node to relay the return value of a GET request.
    This function waits for the result (value and storage ID) and forwards it
    to the command node for output.
    """
    if rank == 0:  # Only the head node relays GET results
        result = MPI.COMM_WORLD.recv(source=MPI.ANY_SOURCE, tag=RETVAL)  # Receive value and ID from storage node
        MPI.COMM_WORLD.send(result, dest=num_processes - 1, tag=RETVAL)  # Send result to the command node

# --- Message Handling Loop ---

def handleMessages():
    """
    Purpose: Main loop that runs on all nodes (except command node).
    This loop continuously listens for incoming MPI messages and dispatches
    them to the appropriate handler based on the tag.

    Design rationale: Uses MPI probe to identify the type and sender of a message
    before actually receiving it, allowing flexible, tag-based dispatching.
    """
    status = MPI.Status()  # Create MPI status object to retrieve metadata from probe
    while True:  # Infinite loop to handle messages
        MPI.COMM_WORLD.probe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)  # Block until any message arrives
        source = status.Get_source()  # Get rank of message sender
        tag = status.Get_tag()  # Get message tag (defines message type)

        # Dispatch message based on tag value
        if tag == END:
            shutdown()  # Shutdown this node (or all nodes if head)
        elif tag == ADD:
            add_node(source)  # Add a new node to the DHT
        elif tag == REMOVE:
            remove_node(source)  # Remove a node from the DHT
        elif tag == PUT:
            put_key_value(source)  # Handle PUT command
        elif tag == GET:
            get_key_value(source)  # Handle GET command
        elif tag == ACK:
            forward_ack_to_client()  # Forward ACK to command node
        elif tag == RETVAL:
            forward_retval_to_client()  # Forward GET result to command node
        elif tag == GET_STORAGE_ID:
            respond(source)  # Respond with this node's storage ID
        elif tag == INIT_NODE:
            init_node(source)  # Initialize new storage node
        elif tag == UPDATE_PARENT:
            update(source)  # Update this node’s parent ID
        elif tag == REDIST:
            receive_keys(source)  # Receive redistributed key-value pairs
        elif tag == REMOVE_NODE:
            remove_help(source)  # Help remove this node
        else:
            print(f"[Rank {rank}] ERROR: Unexpected tag {tag}")  # Error for unknown tags
            sys.exit(1)  # Exit with error

# --- Main Program Entry ---
if __name__ == "__main__":
    num_processes = MPI.COMM_WORLD.Get_size()  # Get total number of MPI processes
    rank = MPI.COMM_WORLD.Get_rank()  # Get this process's rank

    if rank == 0:  # Initialization for head node
        node_id = 0  # Head node ID is 0 by spec
        child_rank = num_processes - 2  # Its child is the permanent node at rank N-2
        node_active = True  # Head node is always active

    elif rank == num_processes - 2:  # Initialization for permanent storage node
        node_id = MAX  # It always holds ID = 1000 (MAX)
        parent_id = 0  # Parent is the head node
        child_rank = 0  # Its child wraps around to the head node
        node_active = True  # This node is always active

    if rank < num_processes - 1:  # For all ranks except the command node
        handleMessages()  # Start the message loop
    else:
        commandNode()  # The command node executes commandNode logic