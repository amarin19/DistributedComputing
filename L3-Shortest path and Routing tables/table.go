package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"time"
	"net"
	"os"
	"strings"
	"strconv"
)

type Node struct {
    //Our processes (that we call nodes) will have the parameters as defined below:
	IP	string
	Port	string
	Id	string
	Initiator bool
	Round string
}

type Tuple struct {
    //Tuple to put inside the map
    ID string
    distance int
}

func main() {
    //1. Read commandline configuration file name and store the nodes.
	file := os.Args[1]
	nodes := ReadConfig(file) //Read the configuration tex file
	table := make(map[string]Tuple) //Create the table initially empty
    
	//2. Open the port
	ln, err := net.Listen("tcp", nodes[0].Port)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	fmt.Printf("TCP successfully opened at %s!\n", nodes[0].Port)

	//3. Create channel for received messages and a counter for the killed processes
	channel_m := make(chan string)
	counter := 0
	n := &counter
    
	//4. Start the message handler.
	go HandleMessage(nodes, channel_m, ln, table, n)
	
	//5. For each neighbour: go dialing and keep on listening (at the 
	//node).
	if nodes[0].Initiator{
	    for _, node := range nodes[1:]{
	        go Dialing(node, nodes[0], nodes[0].Id, 1, false, n)
	        go Listening(ln, channel_m)
	    }
	}else{ //If the node is passive, just listen
		for i := 0; i<len(nodes[1:]); i++{
	        go Listening(ln, channel_m)
	    }
	}
	
	//5. Main loop: processes do not know if the algorithm has stabilized.
	//Reader to wait for a killing signal
	reader := bufio.NewReader(os.Stdin)
	text := ""
	for{
		//This if will wait until a valid exit signal is input.
		if text == "" {
		    text, _ = reader.ReadString('\n')
	        text = strings.Replace(text, "\n", "", -1)
	        if text == "e" {
	            for _, node := range nodes[1:]{
	                go Dialing(node, nodes[0], nodes[0].Id, 0, true, n)
	            }
	        }else{
	            //ERROR: not a valid exit signal. So the program will continue.
	            text = ""
	        }
	    }
	    //After it has received it, it will wait until it has forwarded the exit
	    //sigal to all neighbours:
	    if counter == len(nodes)-1 {
	        fmt.Println("Process",nodes[0].Id,"exiting...")
	        break
	    }
    }
}

func ReadConfig(file string) []Node {
    //This function reads the configuration file and returns a list of node
    //struct.
    
    //1. Check if the function can read the file. If it cant, exit the script.
    b, err := ioutil.ReadFile(file)
    if err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
    
    //2. Convert the file in a string and split them by newline command "\n".
    str := string(b)
	lines := strings.Split(str, "\n")
	
	//3. Main operation to parse the text:
	c := make([]Node, 1)
	for n, l := range lines[:len(lines)-1] {
	    //Split the lines by ":"
		a := strings.Split(l, ":")
		
		//If n == 0 it's the local port. We need to treat it differently.
		if n == 0 {
		    //By default, a node is not an initiator, but we can check if it is.
		    initiator := false
		    if len(a) == 4 {
		        initiator = true
		    }
			//Store the first node.
			c[n] = Node{a[0],":"+a[1],a[2],initiator,"0"}
		}else {
		    //Store the rest of nodes.
			c = append(c, Node{a[0],":"+a[1], "", false, ""})
		}
	}
    //4. Return the list of nodes.
	return c
}

func Dialing(node, self Node, init_ID string, d int, end bool, n *int) {
    //This function dials the IP:Port address until it is connected and sends 
    //the connection through a channel. It dials every 2 seconds.
    
    //1. Store the IP:Port address
    IP := node.IP
    Port := node.Port
    self_ID := self.Id
    round := self.Round
    dist_string := strconv.Itoa(d)
    
    //2. Main function loop:
	for {
	    //a. Ensure the connection.
		ln, err := net.Dial("tcp", IP+Port)
		if err != nil {
		    time.Sleep(2*time.Second) //Try to connect every 2 seconds
			fmt.Println(err)
		}
		//b. When the connection is done, print it. After finishing, close it.
		if ln != nil {
		    //End signal is present? If so, execute this and close.
		    if end {
		        fmt.Printf("Dialed exiting terminal at %s%s!\n", IP,Port)
    		    fmt.Fprintf(ln, "e\n")
    		    fmt.Println("Exiting signal sent!")
    		    *n++
    	       	ln.Close()
    		break
		    }else{
    			fmt.Printf("Dialed connection at %s%s!\n", IP,Port)
    		    fmt.Fprintf(ln, self_ID+":"+init_ID+":"+dist_string+":"+round+"\n")
    		    fmt.Println("Message sent at round "+round+"! ("+self_ID+":"+init_ID+":"+dist_string+")")
    	       	ln.Close()
    		break
   		    }
	    }
	}
}

func Listening(ln net.Listener, m chan string) {
    //This function uses the net listener to keep on listening while the script
    //is running and sends the connection through a channel.
    for {
        //a. Ensure the connection.
        conn, err := ln.Accept()
    	if err != nil {
    		fmt.Println(err)
        }
        //b. When the connection is established, print the received message and send it to the
        //messages channel.
    	if conn != nil{
			fmt.Print("Listened connection! ")
			message, _ := bufio.NewReader(conn).ReadString('\n')
			fmt.Println(message)
		   	m <- message
			conn.Close()
    	}
	}
}

func HandleMessage(nodes []Node, r chan string, ln net.Listener, table map[string]Tuple, n *int) {
    //Function that handles the messages received in the channel. It is the core
    //function that makes everything work smoothly.
    for {
        //1. Start reading the messages.
        m :=<- r
        myRound, _ := strconv.Atoi(nodes[0].Round)
        //If the message is not an exit signal...
        if m != "e\n"{
            //1. Split it and store key values:
            a := strings.Split(m, ":")
            recv_ID := a[0]
            init_ID := a[1]
            recv_dist, _ := strconv.Atoi(a[2])
            recv_round, _ := strconv.Atoi(a[3][:len(a[3])-1])
            
            //2. If the value is not myself:
            if nodes[0].Id != init_ID {
                //3. If the key exists...
	            if stored_d := table[init_ID].distance; stored_d !=0  {
	                //4. If the distance I had is bigger than the one I'm getting...
	                if stored_d > recv_dist {
	                    //5. If I'm from a lowe round:
	                    if myRound < recv_round {
	                        //a. Update my round and get a clean table
                            nodes[0].Round = a[3][:len(a[3])-1]
                            table = make(map[string]Tuple)
                            //b. Tell all my neighbours I restarted (distance = -1)
                            for _, node := range nodes[1:]{
                                go Dialing(node, nodes[0], nodes[0].Id, -1, false, n)
        	                    go Listening(ln, r)
	                        }
	                    }
	                    //a. Correct restarted received value:
	                    if recv_dist == -1{
	                        recv_dist = 1
	                    }
	                    
	                    //b. Update next hop and distance in table and tell my neighbours:
	                    tuple := Tuple{recv_ID, recv_dist}
	                    table[init_ID] = tuple
	                    for _, node := range nodes[1:]{
	                        if node.Id != recv_ID {
	                            go Dialing(node, nodes[0], init_ID, recv_dist+1, false, n)
	                            go Listening(ln, r)
	                        }
	                	}
	                } else {
    	                //Do nothing but listen
	                    go Listening(ln, r)
	                }            
	            }else{
    	            //a. The key is not in my hash table yet. If needed, correct
    	            //the distance...
                    if recv_dist == -1{
	                    recv_dist = 1
	                }
	                //...and insert next hop + distance in my table and inform
	                //my neighbours
	                tuple := Tuple{recv_ID, recv_dist}
	                table[init_ID] = tuple
	                for _, node := range nodes[1:]{
	                    if node.Id != recv_ID {
	                        go Dialing(node, nodes[0], init_ID, recv_dist+1, false, n)
	                        go Listening(ln, r)
	                    }
	                }
	            }
	        }
	        // Print the table so I have a control of the situation:
	        fmt.Println(table)
        }else{
            //Some of my neighbour has been killed. I count him dead...
            *n++
            //...and update my round, clean the table and send everyone I restart
            nodes[0].Round = strconv.Itoa(myRound+1)
            table = make(map[string]Tuple)
            for _, node := range nodes[1:]{
                go Dialing(node, nodes[0], nodes[0].Id, -1, false, n)
        	    go Listening(ln, r)
	        }
	    }
    }
}
