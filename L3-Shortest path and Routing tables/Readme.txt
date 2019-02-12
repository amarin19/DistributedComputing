1.TEAM MEMBERS:
Marc Alvinyà Rubió
Albert Marin Sierra

2. COMPILATION:
Simply run "./exeShell" in a terminal and everything will happen.

3. EXTRA NEEDED INFO:
It can be executed one by one by running "go run table.go <config_file.txt>"
for each node in a new tab in the terminal. If the optional exercise wants to be
executed, "./exeShellExtra" will call the "table_extra.go"

The algorithm goes as follows: the message that is being sent is <id, id of the 
first sender, distance so far, round> except when a process is killed. In this
case, the message is just "e". To kill a process, just write "e" in the shell
and hit enter. When a process receives a message:

- If the message is "e" -> a neighbour has been killed. The process then changes
his round to round+1, empties his table and sends everyone his id twice, 
distance = -1 and the new round number.
- If the message is not "e":
    - If id == id of the first sender -> It means it's his own message coming back and
    discards the message.
    - If id != id of the first sender: 
        - If the node has not been stored in the node's hash table yet, if the
            distance is -1 it means that some process has received an exit signal from a neighbour.
            Then, it changes this distance to 1, proceeds to store the distance and forwards
            the message to all his neighbours with new distance = distance+1.
        - If it has been stored already:
            - If round of the process < round, the process updates his round to this
                new one, empties his table and sends a message to all his neighbours with 
                his id twice, distance = -1 and the new round number.
            - If distance = -1, it means that someone's neighbour has been killed.
            Change distance to 1 and continue to the next bullet.
            - Update the new value in the table and inform everone of this change.
