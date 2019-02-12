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
    "math/rand"
)

type Node struct {
    //Our processes (that we call nodes) will have the parameters as defined below:
	IP	string
	Port	string
	Id	string
    N string //network size
	Initiator bool
	Parent string
	CurrentWave string
    CurrentRound string
    subTree_size string
}

func main() {
    //1. Read commandline configuration file name and store the nodes.
	file := os.Args[1]
	nodes := ReadConfig(file)
    
	//2. Open the port
	ln, err := net.Listen("tcp", nodes[0].Port)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	fmt.Printf("TCP successfully opened at %s!\n", nodes[0].Port)

	//3. Create channel for received messages.
	channel_m := make(chan string)

	//4. Create a counter to store neighbours that have replied to the node.
	counter := make([] int, 0)
	p := &counter
	ending := false
	e := &ending

	//5. Start the message handler.
	go HandleMessage(nodes, channel_m, p, e, ln)
	
	//6. For each neighbour: go dialing and keep on listening (at the 
	//node).
	if nodes[0].Initiator{
        N, _ := strconv.Atoi(nodes[0].N)
        rand.Seed(time.Now().Unix())
        CW := rand.Intn(N)+1 //Generate random ID in range [1,N]
        fmt.Println("Randomly choosen ID: ", CW)
        nodes[0].CurrentWave = strconv.Itoa(CW)
	    for _, node := range nodes[1:]{
	        go Dialing(node, nodes[0], false)
	        go Listening(ln, channel_m)
	    }
	}else{ //If the node is passive, just listen
		for i := 0; i<len(nodes[1:]); i++{
	        go Listening(ln, channel_m)
	    }
	}
	
	//7. Main loop: work until all nodes have been visited.
	for{
	    if *e {
            fmt.Println("Terminated.")
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
		    if len(a) == 5 {
		        initiator = true
		    }
			//Store the first node.
			c[n] = Node{a[0],":"+a[1],":"+a[2],a[3],initiator, "", "", "0", "0"}
		}else {
		    //Store the rest of nodes.
			c = append(c, Node{a[0],":"+a[1],"","",false, "", "", "0", "0"})
		}
	}
    //4. Return the list of nodes.
	return c
}

func Dialing(node, self Node, end bool) {
    //This function dials the IP:Port address until it is connected and sends 
    //the connection through a channel. It dials every 2 seconds.
    
    //1. Store the IP:Port address. If we pass the same node twice, it will
    //call the parent node.
    IP := ""
    Port := ""
    if node != self {
        IP = node.IP
        Port = node.Port
    }else{
        a := strings.Split(self.Parent, ":")
        IP = a[0]
        Port = ":"+a[1]
    }
    
    //2. Control variables for the current node:
    selfIP := self.IP
    selfPort := self.Port
    selfCW := self.CurrentWave
    selfR := self.CurrentRound
    selfP := self.Parent
    selfS := self.subTree_size
    
    //3. Main function loop:
	for {
	    //a. Ensure the connection.
		ln, err := net.Dial("tcp", IP+Port)
		if err != nil {
		    time.Sleep(2*time.Second)
			fmt.Println(err)
		}
		//b. When the connection is done, print it. After finishing, close it.
		if ln != nil {
			fmt.Printf("Dialed connection at %s%s!\n", IP,Port)
			//b1. If we have the leader, send the stop. This is triggered by the
			//signal "end = true". Else, send the IP:Port:wave number.
			if end{
			    fmt.Fprintf(ln, "STOP\n")
			}else{
			    fmt.Fprintf(ln, selfIP+selfPort+":"+selfR+":"+selfCW+":"+selfS+"\n")
    			if selfP != (IP+Port){
    				fmt.Println("Message sent! Wave = (",selfR,",",selfCW,",",selfS,")")
        		}else{
        		    //b2. When we are sending the message to the parent it is
        		    //because the wave has finished in the children side.
        	    	fmt.Println("Wave = (",selfR,",",selfCW,",",selfS,")"," done! Waiting for stop...")
    		    }
    		}
    		ln.Close()
			break
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
        //b. When it is ready, print the received message, send it to the
        //messages channel and close the connection.
    	if conn != nil{
			fmt.Print("Listened connection! ")
			message, _ := bufio.NewReader(conn).ReadString('\n')
			fmt.Println(message)
		   	m <- message
			conn.Close()
    		break
    	}
	}
}

func HandleMessage(nodes []Node, r chan string, counter *[]int, e *bool, ln net.Listener) {
    //Function that handles the messages received in the channel. It is the core
    //function that makes everything work smoothly.
    for {
        //1. Start reading the messages.
        m :=<- r
        
        //1a. If it is a stop message, nodes know the leader. So send it to the 
        //children, append values to the counter to trigger the ending of the 
        //script and exit this loop.
        if m == "STOP\n" {
            for _, node := range nodes[1:] {
                if (node.IP+node.Port) != nodes[0].Parent{
                    Dialing(node, nodes[0], true)
                }
            }
            fmt.Println("Closing connections... leader = from round", nodes[0].CurrentRound+", ID: "+nodes[0].CurrentWave+".")
            *counter = append(*counter, 1,1)
            *e = true
            break
        //1b. If it is a non-empty message, it is a IP:Port:CurrentRound:CurrentWave:subTree_size
        }else if m != ""{
            //1. Split it and store key values:
            a := strings.Split(m, ":")
            Round,_ := strconv.Atoi(a[2])
            Id, _ := strconv.Atoi(a[3])
            selfCW, _ := strconv.Atoi(nodes[0].CurrentWave)
            selfR, _ := strconv.Atoi(nodes[0].CurrentRound)
            subTree_size,_ := strconv.Atoi(a[4][:len(a[4])-1])

            //2. Main switch as defined in the slides.
          	switch {
                case (selfR < Round) || (Round == selfR && selfCW < Id):
                    //1. The wave I'm in is smaller than the wave I receive. So
                    //I switch to the new one, updating my parent, current wave
                    //and resetting the counter to 0.
    	            IP := a[0]
    	            Port := ":"+a[1]
            	    nodes[0].Parent = IP+Port
            	    nodes[0].CurrentWave = strconv.Itoa(Id)
                    nodes[0].CurrentRound = strconv.Itoa(Round)
                    nodes[0].subTree_size = strconv.Itoa(subTree_size)
            	    fmt.Print("My parent is: ", nodes[0].Parent)
            	    fmt.Print(" and my curent wave is: (",nodes[0].CurrentRound,",",nodes[0].CurrentWave,",",nodes[0].subTree_size,")")
            	    fmt.Print(".\n")
            	    *counter = make([]int, 0)
            	    //2. I need to send each of my new non-parent a message and
            	    //open new listening channels.
                    for _, node := range nodes[1:] {
                        if (node.IP+node.Port) != nodes[0].Parent{
                            go Dialing(node, nodes[0], false)
                            go Listening(ln, r)
                        }
                    }
                    //3. If I don't have neighbours, then I have to send it back
                    //to my parent and I keep a listening side to wait for the 
                    //end signal.
                    if len(nodes)-2 == 0 {
                        s, _ :=  strconv.Atoi(nodes[0].subTree_size)
                        s++
                        nodes[0].subTree_size = strconv.Itoa(s)
                        go Dialing(nodes[0], nodes[0], false)
                        go Listening(ln, r)
                    }
                case (selfR > Round) || (selfR == Round && selfCW > Id):
                    //2. The wave I'm in is greater than the one I receive. I 
                    //ignore it but then I will have lost a listener connection 
                    //so I re-open one.
                    go Listening(ln, r)
                default:
                    //3. The default is when the wave is the same as the I am currently in. 
                    //Then, I count one message.
                    *counter = append(*counter, 1)
                    s, _ :=  strconv.Atoi(nodes[0].subTree_size)
                    //If I don't have a parent, it's because I might be the 
                    //leader so I wait until I get all my
                    //neighbouring messages. When I get them, I announce I'm the
                    //leader, count another 1 to send the termination signal and
                    //break this loop.
                    if nodes[0].Parent == "" {
                        if len(*counter) != (len(nodes)-1) {
                            nodes[0].subTree_size = strconv.Itoa(subTree_size+s)
                            continue
                            //Initiate new wave
                        }else{
                            N,_ := strconv.Atoi(nodes[0].N)
                            if (subTree_size+s) == N-1 {
                                fmt.Print("LEADER FOUND = ", Id)
                                fmt.Println(". I'm the leader!")
                                for _, node:= range nodes[1:] {
                                    Dialing(node, nodes[0], true)
                                }
                                *e = true
                                break
                            }else{
                                selfR++
                                rand.Seed(time.Now().Unix())
                                CW := rand.Intn(N)+1 //Generate random ID in range [1,N]
                                fmt.Println("New random ID: ", CW)
                                *counter = make([]int, 0)   
                                nodes[0].Parent = ""
                                nodes[0].CurrentWave = strconv.Itoa(CW)
                                nodes[0].CurrentRound = strconv.Itoa(selfR)
                                nodes[0].subTree_size = strconv.Itoa(0)
                                for _, node:= range nodes[1:] {
                                    go Dialing(node, nodes[0], false)
                                    go Listening(ln, r)
                                }
                            }
                        }
                    }else{
                        //If I have a parent, I just wait until I have all the
                        //neighbouring messages. When I have them, I dial my
                        //parent and wait for a message of him.
                        if len(*counter) != (len(nodes)-2) {
                            nodes[0].subTree_size = strconv.Itoa(subTree_size+s)
                            continue
                        }else{
                            s++
                            nodes[0].subTree_size = strconv.Itoa(subTree_size+s)
                            go Dialing(nodes[0], nodes[0], false)
							go Listening(ln, r)
                        }    
                    }
                }
        }else{
            //If nothing is received, I wait for 2 seconds and read the channel
            //again.
            time.Sleep(2*time.Second)
        }
    }
}
