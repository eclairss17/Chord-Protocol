#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka
open Akka.Actor
open Akka.FSharp

#time "on"


let system =
    System.create "system" (Configuration.defaultConfig ())
let mutable num = 0
let mutable chordMap = Map.empty
let mutable keyMap = Set.empty
// let mutable searchMap = Map.empty
let mutable keyfound = false
let arbitrary = Random()

type MessageObject =
    | ChordMapping of Map<int, IActorRef>*int
    | ChangeSuccessor of Map<int, IActorRef>*int*int
    | ChangePredecessor of  Map<int, IActorRef>*int*int
    | KeyDistribute of int*int
    | Callsuccessor of int
    | FingerTable of Map<int, IActorRef>*int
    | DeadNodeKeyTransfer
    | ReceiveKeysFromDeadNode of Set<int>
    | ReceiveFingerTableFromDeadNode of Map<int, IActorRef>* Map<int, IActorRef>
    | DeadNodeFingerTableTransfer
    | SearchKey of int
    | SearchInFingerTable of int
    | ReturnNumberofHops


let player(mailbox: Actor<_>) =
    let mutable numberOfHops = 0
    let mutable successor = Map.empty
    let mutable successorReceived = Map.empty
    let mutable lastSecondNodeCheck = Map.empty
    let mutable predecessor = Map.empty
    let mutable keys = Set.empty
    let mutable selectednodes = Set.empty  
    let mutable totalplayers = 0
    let mutable nodeid= 0
    let mutable requestedKeyFound = false
    let mutable temp = -1
    
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


            | KeyDistribute(keyid,nodeidOfKey) -> 
                let mutable onlyonce = true  
                
                
                    // printfn " halt 1 ---------- node %i" nodeid
                if not (nodeidOfKey = totalplayers) then
                    for KeyValue(k,v) in successor do
                        if onlyonce then
                            // printfn "callsuccessor---> %i" k
                            if(k>nodeidOfKey) then
                                onlyonce<- false
                                chordMap.[k] <! Callsuccessor(keyid) 
                    
                else 
                    keys<-keys.Add(keyid)   
                    // printfn "key %i -*-> Node %i" keyid nodeid 
                    printf "keys %A" keys 


            
            | Callsuccessor(keyid) ->    
                keys<-keys.Add(keyid)
                // printfn "key %i *-*>* Node %i" keyid nodeid         

            | DeadNodeKeyTransfer -> 
                send <! keys
            
            | ReceiveKeysFromDeadNode(keysetToAdd)->
                // printfn "----------------------- settttt for %i ---%A" nodeid keysetToAdd 
                if not (keysetToAdd.IsEmpty) then
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
            | DeadNodeFingerTableTransfer ->                 
                send <! successor
            | ReceiveFingerTableFromDeadNode(fingerTableFromFailedNode,chordMap) -> 
                let mutable flag= true 
                // printfn "Old Finger table for node %i is ---- %A" nodeid successor
                let join (p:Map<'a,'b>) (q:Map<'a,'b>) = 
                    Map(Seq.concat [ (Map.toSeq p) ; (Map.toSeq q)])                
                successorReceived<- join successorReceived fingerTableFromFailedNode
                successorReceived<- successorReceived.Remove nodeid
                successor<- join successorReceived successor
                
            // | SearchKey(chordMap,keyid) -> 
            //     if(keys.Contains(keyid)) then 
            //         printfn "Key exists at node ----- %i " nodeid

            //     else if nodeid = totalplayers then
            //         printfn "Key Node Found"
            //         // while not keyfound do 
            //     else 
            //         for KeyValue(k,v) in successor do 
            //             chordMap.[k] <! SearchKey(chordMap,keyid)
            //             printfn "searching at node %i" k

            // | SearchMethod2(keyid) ->
            //     printfn " key found -- %b" keyfound
            //     if (keys.Contains(keyid)) then 
            //         printfn "Key exists at node ----- %i " nodeid
            //         keyfound<- true
            //     else if nodeid = totalplayers then
            //         printfn "Key Node Found"
            //     else if not keyfound then 
            //         for KeyValue(k,v) in successor do
            //             printfn "sending key for next search------- %i" k
            //             if not(k = totalplayers) then
            //                 chordMap.[k] <! SearchMethod2(keyid)
            //     else 
            //         printfn "Key Node Found at node %i"
                        // send <! k
            
            | SearchKey (keyid)-> 
                //check whether Key exists in the key set or not
                //check whether the key exists in the fingertable or not
                
                let mutable fingerTableSearch = 2 
                
                if(keys.Contains(keyid)) then 
                    printfn "Node %i Found Key** %i" nodeid keyid
                    fingerTableSearch<-1
                    if (fingerTableSearch =1 ) then
                        send<! fingerTableSearch
                else 
                    // send<! false 

                    let mutable flag = false
                    

                    while not flag do
                        for KeyValue(k,v) in successor do
                            if k <= keyid then
                                temp <- k
                        printfn " Temp---%i" temp
                        fingerTableSearch <- Async.RunSynchronously(chordMap.[temp] <? SearchInFingerTable(keyid),2000)
                        if(fingerTableSearch = 1) then
                            requestedKeyFound <- true
                            temp<- -1
                            flag<- true
                        else
                            
                            if(chordMap.ContainsKey(temp)) then
                                chordMap.[temp] <! SearchKey(keyid)

                    // for KeyValue(k,v) in successor do
                            // fingerTableSearch<-2
                            // if (k <= keyid) &&  (fingerTableSearch <> 1) then
                            //     numberOfHops 1<- numberOfHops + 1                           
                            //     fingerTableSearch <- Async.RunSynchronously(chordMap.[temp] <? SearchInFingerTable(keyid),2000)
                            //     if(fingerTableSearch = 1) then
                            //         requestedKeyFound <- true
                            //     printfn "callingSearchOn--->%i isKeyFound-%i" k fingerTableSearch

                            // else if(k > keyid) &&  (fingerTableSearch <> 1) then                             
                            //     if (k = totalplayers) then 
                            //         fingerTableSearch <- 0
                            //         numberOfHops <- 0
                            //         printfn "CheckResponse -------%i" nodeid
                            //         send <!  nodeid
                            //     if (chordMap.ContainsKey(k)) then
                            //         printfn "callingSearchOnooohlalala--->%i" k
                            //         fingerTableSearch <- Async.RunSynchronously(chordMap.[k] <? SearchInFingerTable(keyid),2000)
                            //         if(fingerTableSearch = 2) then 
                            //             chordMap.[k] <! SearchKey(keyid) 
                            //         else 
                            //             if(fingerTableSearch = 1) then
                            //                 requestedKeyFound <- true
                            //             printfn "Node-%i found key-%i" nodeid keyid
                            //             send <! fingerTableSearch

                            // if fingerTableSearch=1 then 
                            //     send<! 1 
                            // else
                            //     numberOfHops<-0 
                            //     send<! 2 
                           
                       
            | SearchInFingerTable (keyid)->
                if (keys.Contains(keyid)) then 
                    printfn "actor-%i key-%i" nodeid keyid
                    send<! 1
                else 
                    send<! 2
            
            | ReturnNumberofHops ->
                send<! numberOfHops


            return! loop ()     
    }
    loop()


let Chordinitiate (totalNodes: int) =
    num<-totalNodes
    for var = 1 to num do
        chordMap <- chordMap.Add(var, spawn system (sprintf "actor%i" var) player)

    
    for var = 2 to num do
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
    
    let mutable LowestKey = 0
    // Select which nodes to send the Keys
    let mutable randNode = 0
    let mutable onlyonce = true
    while not (keyMap.IsEmpty)  do
        LowestKey<- Set.minElement keyMap
        // let mutable randNode = arbitrary.Next(1,(updateUpperBound)+1)
        // printfn " upper bound kya h %i" updateUpperBound 
    
        // if (randNode <= updateUpperBound) then
            // let mutable randKey = arbitrary.Next(randNode,(updateUpperBound)+1)
            // printfn "key ki range kya h randnode--- %i---upperbound %i---" randNode ((updateUpperBound)+1)
            // while not(chordMap.ContainsKey(randNode)) do
            //     randNode<-randNode + 1           
        // if keyMap.Count > 1 then 
        //     onlyonce<- true
        // for i in keyMap do
            // let mutable baseCondition = true
            
        if onlyonce then
            randNode <- LowestKey
                // printfn " PlanKey%i-------------Node%i " i randNode
            onlyonce<-false               
            
        if (keyMap.Contains(LowestKey)) && chordMap.ContainsKey(randNode) then
            // printfn "SendingKey%i-------------Node%i" LowestKey randNode               
            chordMap.[randNode] <! KeyDistribute(LowestKey,randNode)            
            keyMap<-keyMap |> Set.remove LowestKey
            // printfn " %A" keyMap
            onlyonce<-true    
        else                 
            randNode<- randNode+1
             
    
    // let mutable deleteset = Set.empty
    // for var = 1 to 5 do 
    //     let mutable nodeexists = true   
    //     let mutable nextnodeId = 0 
    //     let mutable prevnodeId = 0  
    //     let mutable findnext = false   
    //     let mutable delete =  arbitrary.Next(2,num-1)
    //     printfn "deleting nodee ------------> %i" delete
    //     while (nodeexists) do 
    //         if not (findnext) then
    //             nextnodeId<- delete + 1
    //             prevnodeId<- delete - 1
            
    //         if (chordMap.ContainsKey(nextnodeId) && chordMap.ContainsKey(prevnodeId) && chordMap.ContainsKey(delete) ) then 
    //             // printfn "Initiating delete for %i---p%i---n%i  " delete prevnodeId nextnodeId
    //             chordMap.[prevnodeId] <!  ChangeSuccessor(chordMap, nextnodeId, delete)
    //             let response = Async.RunSynchronously((chordMap.[delete] <? DeadNodeKeyTransfer), 2000)
    //             // printfn " Response-----%i---%A" delete response
    //             chordMap.[nextnodeId] <! ReceiveKeysFromDeadNode(response)
    //             if not (delete=num) then
    //                 let response2 = Async.RunSynchronously((chordMap.[delete] <? DeadNodeFingerTableTransfer), 2000)
    //                 chordMap.[nextnodeId] <! ReceiveFingerTableFromDeadNode(response2,chordMap)
    //             chordMap.[nextnodeId] <!  ChangePredecessor(chordMap, prevnodeId, delete)
    //             // printfn "Delete Complete for %i " delete
    //             chordMap<-chordMap.Remove delete
    //             nodeexists<- false

    //         else 
    //             let mutable stop = false
    //             if (deleteset.Contains(delete) ) then
    //                 nodeexists<-false 
    //                 stop<- true
    //             if not (chordMap.ContainsKey(nextnodeId)) && not stop then
    //                 findnext<-true
    //                 nextnodeId<-nextnodeId+1
    //                 // printfn "next node %i" nextnodeId
    //             if not (chordMap.ContainsKey(prevnodeId)) && not stop then
    //                 findnext<-true
    //                 prevnodeId<-prevnodeId-1  
    //                 // printfn " previousnode %i " prevnodeId

    //     deleteset<-deleteset.Add(delete)
        // printfn "delete set %A " deleteset
    Console.WriteLine("Enter Number of Requests") 
    let numberOfRequests = Console.ReadLine()
    let requests= int(numberOfRequests)
    // while not (requests = 0) then
    let mutable nodeselect =  arbitrary.Next(1,num+1)
    printfn "nodeselect kiya --->%i" nodeselect
    let mutable response3 = 0
    if (chordMap.ContainsKey(nodeselect)) then 
    
        // if not (keyfound) && (nodeselect<chordMap.Count-int(numberOfRequests)) then       
        let mutable findKey = arbitrary.Next(nodeselect,num)
        printfn "Initiating search for %i---k%i " nodeselect findKey
        response3<- Async.RunSynchronously (chordMap.[nodeselect] <? SearchKey(findKey),2000) 
        // printfn "response----> %i" response3
        
        // if (response3 = 1) then 
        // keyfound<- true
        // printfn "Search Works"
    //         printfn "response %A" response3
    //         // response3 <- Async.RunSynchronously(chordMap.[response3] <? SearchMethod2( int(keyid)),2000)
    // else 
    //     nodeselect<- nodeselect-1 


    printfn "Terminate"
Chordinitiate(15)
    
    // let mutable sample = Map.empty

    // sample<-sample.Add(1,[1])
    // sample<-sample.Add(2,[3])
    // sample<-sample.Add(3,[34])
    // printfn "Set a: %O " sample
    // // Map.iter (fun x -> printf "%O " x) sample
    // if(sample.ContainsKey(2)) then 
    //     sample<- sample |> Map.remove 2
    // printfn "Set a: %O " sample
    
    // let merge (a : Map<'a, 'b>) (b : Map<'a, 'b>) (f : 'a -> 'b * 'b -> 'b) =
    // Map.fold (fun s k v ->
    //     match Map.tryFind k s with
    //     | Some v' -> Map.add k (f k (v, v')) s
    //     | None -> Map.add k v s) a b

    // let a = Map([1,11;2,21;3,31;])

    // let b = Map([3,32; 4,41;5,51;6,61;])
    // let map = merge a b (fun k (v, v') -> v + v');;
    // printfn " %A -----" map

    // let join (p:Map<'a,'b>) (q:Map<'a,'b>) = 
    //     Map(Seq.concat [ (Map.toSeq p) ; (Map.toSeq q) ])
    // let a = Map([1,11;2,21;3,31;])

    // let b = Map([3,32; 4,41;5,51;6,61;])

    // let c = join b a
    // printfn "%A" c
    


    // Console.WriteLine( sample.[2])

