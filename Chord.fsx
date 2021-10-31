#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#time "on"

open System
open Akka.Actor
open Akka.FSharp


let system =
    System.create "system" (Configuration.defaultConfig ())
let mutable num = 0
let mutable chordMap = Map.empty
let mutable keyMap = Set.empty
let arbitrary = Random()

type MessageObject =
    | ChordMapping of Map<int, IActorRef>*int
    | ChangeSuccessor of Map<int, IActorRef>*int*int
    | ChangePredecessor of  Map<int, IActorRef>*int*int
    | KeyDistribute of int
    | FingerTable of Map<int, IActorRef>*int
    | DeadNodeKeyTransfer
    | ReceiveKeysFromDeadNode of Set<int>

let player(mailbox: Actor<_>) =
    let mutable successor = Map.empty
    let mutable predecessor = Map.empty
    let mutable keys = Set.empty
    let mutable totalplayers = 0
    let mutable nodeid= 0
    let rec loop () = 
        actor {
            let! msg = mailbox.Receive()
            let send = mailbox.Sender()
            match msg with
            | ChordMapping (chordMap, nodeId)-> 
                nodeid<-nodeId
                totalplayers<-chordMap.Count
                // printfn "Totalplayers - %i current id- %i" totalplayers nodeId
                if not (nodeid = totalplayers) then
                    successor<- successor.Add(nodeid+1,chordMap.[nodeid+1]) 
                    // printfn "Successor of Nodeid %i is %A " nodeid successor.[nodeid+1]
                if not (nodeid = 1) then 
                    predecessor<- predecessor.Add(nodeid-1,chordMap.[nodeid-1] )
                    // printfn "Predecessor of Nodeid %i is %A " nodeid predecessor.[0]
                mailbox.Self <! FingerTable(chordMap,totalplayers)

            | ChangeSuccessor(chordMap, newsuccessorid, deletenode)->
                //node i is deleted so change the successor of i-1 from i to i+1
                let mutable flag = true
                // printfn "HH kya %b " (successor.ContainsKey(deletenode) )
                while flag do
                    // printfn "Deleting node %i and changing the new successor for %i to this -- %i" deletenode nodeid newsuccessorid
                    if ( successor.ContainsKey(deletenode) ) then 
                        successor<- successor |> Map.remove (deletenode)
                        successor<- successor.Add(newsuccessorid,chordMap.[newsuccessorid]) 
                        // printfn "After deleting node %i new successor node is %i" deletenode newsuccessorid
                        
                        flag<-false
            | ChangePredecessor(chordMap, newprevid, deletenode)->
                //node i is deleted so change the predecessor of i+1 from i to i-1
                
                let mutable flag = true
                while flag do
                    
                    
                    if ( predecessor.ContainsKey(deletenode) ) then 
                        predecessor<- predecessor |> Map.remove (deletenode)
                        predecessor<- predecessor.Add(newprevid,chordMap.[newprevid]) 
                        // printfn "After deleting node %i new predecessor node is %i" deletenode newprevid                        
                        flag<-false

            | KeyDistribute(keyid) -> 
                keys<-keys.Add(keyid)
                printfn "Node %i has got the key %i" nodeid keyid

            | DeadNodeKeyTransfer -> 
                send <! keys  
            
            | ReceiveKeysFromDeadNode(keysetToAdd)->
                if not (keysetToAdd.IsEmpty) then
                    printfn "----------------------- settttt for %i ---%A" nodeid keysetToAdd 
                    keys<- Set.union keys keysetToAdd
                    printfn " new set for %i is ==== %A" nodeid keys
                else 
                    printfn "NO KEYS TO SEND"

            | FingerTable(chordMap,totalnodes) -> 

                let mutable flag = true 
                let mutable onlyonce = true 
                for i=0 to totalnodes do
                    let mutable nextnode = (2.0)**float(i) + float(nodeid)
                    if(nextnode <float(totalnodes)) && flag then
                        successor<-successor.Add(int(nextnode),chordMap.[int(nextnode)])
                        // printfn "Finger Table for %i has a new entry %i" nodeid (int(nextnode))
                    else 
                        flag <- false
                        if (onlyonce) then 
                            successor<-successor.Add(totalnodes,chordMap.[totalnodes])
                            // printfn "Finger Table for %i has last entry %i" nodeid totalnodes
                            onlyonce<- false
            return! loop ()     
    }
    loop()


let Chordinitiate (totalNodes: int) =
    num<-totalNodes
    for var = 1 to num do
        chordMap <- chordMap.Add(var, spawn system (sprintf "actor%i" var) player)

    
    for var = 1 to num do
        let state = chordMap
        chordMap.[var] <! ChordMapping(chordMap,var)
    // printfn " All chords initially mapped"

    //  Select the amount of Keys in circulation 
    let amount = arbitrary.Next(1,(num)/3)
    
    // let mutable randNode = Set.empty
    for var = 1 to num do 
        keyMap<-keyMap.Add(var)
        // randNode<-randNode.Add(var)
    // printfn "keyMap------- %A" keyMap
    
    let mutable updateUpperBound = 0
    // Select which nodes to send the Keys
    while not (keyMap.IsEmpty)  do
        updateUpperBound<- Set.maxElement keyMap
        let mutable randNode = arbitrary.Next(1,(updateUpperBound)+1)
        // printfn "randNode -- %i" randNode
        // printfn " upper bound kya h %i" updateUpperBound 
        if (randNode <= updateUpperBound) then
            let mutable randKey = arbitrary.Next(randNode,(updateUpperBound)+1)
            // printfn "randKey kya jari h  -- %i" randKey
            // printfn "key ki range kya h randnode--- %i---upperbound %i---" randNode ((updateUpperBound)+1)
            // while not(chordMap.ContainsKey(randNode)) do
            //     randNode<-randNode + 1           
            if(keyMap.Contains(randKey)) && chordMap.ContainsKey(randNode) then
                // printfn "Sending Key %i to %i " randKey randNode
                chordMap.[randNode] <! KeyDistribute(randKey)
            keyMap<-keyMap |> Set.remove randKey
        // printfn "Set is %A" keyMap
    
    let mutable deleteset = Set.empty
    for var = 1 to 5 do 
        let mutable nodeexists = true   
        let mutable nextnodeId = 0 
        let mutable prevnodeId = 0  
        let mutable findnext = false   
        let mutable delete =  arbitrary.Next(2,num)
        printfn "deleting nodee ------------> %i" delete
        while (nodeexists) do 
            if not (findnext) then
                nextnodeId<- delete + 1
                prevnodeId<- delete - 1
            
            if (chordMap.ContainsKey(nextnodeId) && chordMap.ContainsKey(prevnodeId) && chordMap.ContainsKey(delete) ) then 
                // printfn "Initiating delete for %i " delete
                chordMap.[prevnodeId] <!  ChangeSuccessor(chordMap, nextnodeId, delete)
                let response = Async.RunSynchronously((chordMap.[delete] <? DeadNodeKeyTransfer), 2000)
                chordMap.[nextnodeId] <! ReceiveKeysFromDeadNode(response)
                // printfn "Running loop"
                chordMap.[nextnodeId] <!  ChangePredecessor(chordMap, prevnodeId, delete)
                // printfn "Delete Complete for %i " delete
                chordMap<-chordMap.Remove delete
                nodeexists<- false

            else 
                let mutable stop = false
                if not (chordMap.ContainsKey(nextnodeId)) && not stop then
                    findnext<-true
                    nextnodeId<-nextnodeId+1
                    printfn "next node %i" nextnodeId
                if not (chordMap.ContainsKey(prevnodeId)) && not stop then
                    findnext<-true
                    prevnodeId<-prevnodeId-1  
                    printfn " previousnode %i " prevnodeId
                if (deleteset.Contains(delete) ) then
                    nodeexists<-false 
                    stop <- true

        deleteset<-deleteset.Add(delete)
        // printfn "delete set %A " deleteset

    printfn "Terminate"
    
Chordinitiate(10)
    // let mutable sample = Map.empty

    // sample<-sample.Add(1,[1])
    // sample<-sample.Add(2,[3])
    // sample<-sample.Add(3,[34])
    // printfn "Set a: %O " sample
    // // Map.iter (fun x -> printf "%O " x) sample
    // if(sample.ContainsKey(2)) then 
    //     sample<- sample |> Map.remove 2
    // printfn "Set a: %O " sample

    // Console.WriteLine( sample.[2])

