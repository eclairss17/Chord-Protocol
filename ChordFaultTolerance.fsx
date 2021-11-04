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
let mutable numberOfHops = 0

type MessageObject =
    | ChordMapping of Map<int, IActorRef>*int
    | ChangeSuccessor of Map<int, IActorRef>*int*int
    | ChangePredecessor of  Map<int, IActorRef>*int*int
    | FingerTable of Map<int, IActorRef>*int
    | KeyDistribute of int*int
    | Callsuccessor of int
    | DeadNodeKeyTransfer
    | ReceiveKeysFromDeadNode of Set<int>
    | ReceiveFingerTableFromDeadNode of Map<int, IActorRef>* Map<int, IActorRef>
    | DeadNodeFingerTableTransfer
    | SearchKey of int
    | SearchInFingerTable of int
    | GetSuccessor of int
    | GetActorVal 
    | ResetNumberofHops
    | ReturnNumberofHops
    | SearchNearestNode of int


let player(mailbox: Actor<_>) =
    let mutable successor = Map.empty
    let mutable successorReceived = Map.empty
    let mutable lastSecondNodeCheck = Map.empty
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
                
                if not (nodeid = totalplayers) then
                    successor<- successor.Add(nodeid+1,chordMap.[nodeid+1]) 
                    // printfn "Successor of Nodeid %i is %A " nodeid successor.[nodeid+1]
                if not (nodeid = 1) then 
                    predecessor<- predecessor.Add(nodeid-1,chordMap.[nodeid-1] )
                    // printfn "Predecessor of Nodeid %i is %A " nodeid predecessor.[nodeid-1]
                mailbox.Self <! FingerTable(chordMap,totalplayers)

            | ChangeSuccessor(chordMap, newsuccessorid, deletenode)->
                //node i is deleted so change the successor of i-1 from i to i+1
                let mutable flag = true
                
                while flag do
                   
                    if ( successor.ContainsKey(deletenode) ) then 
                        successor<- successor |> Map.remove (deletenode)
                        successor<- successor.Add(newsuccessorid,chordMap.[newsuccessorid]) 
                        
                        
                        flag<-false
            | ChangePredecessor(chordMap, newprevid, deletenode)->
                //node i is deleted so change the predecessor of i+1 from i to i-1
                
                let mutable flag = true
                while flag do
                    
                    
                    if ( predecessor.ContainsKey(deletenode) ) then 
                        predecessor<- predecessor |> Map.remove (deletenode)
                        predecessor<- predecessor.Add(newprevid,chordMap.[newprevid]) 
                                             
                        flag<-false

            | GetActorVal ->
                send <! nodeid

            | GetSuccessor(nodeId) ->
                let successorNodeRef = successor.[nodeId]
                let successorNode = successorNodeRef <! GetActorVal
                send <! successorNode

            | KeyDistribute(keyid,nodeidOfKey) -> 
                let mutable onlyonce = true  
                
                
                if (nodeidOfKey <> totalplayers) then
                    for KeyValue(k,v) in successor do
                        if onlyonce then
                            if(k>nodeidOfKey) then
                                chordMap.[k] <! Callsuccessor(keyid) 
                                onlyonce<- false                    
                    
                else                 
                    if nodeid <> 0 then
                        keys<-keys.Add(keyid)   
                        // printfn "key %A -*-> Node %i" keys nodeid 
                  


            
            | Callsuccessor(keyid) ->    
                if nodeid <> 0 then
                    keys<-keys.Add(keyid)
                    // printfn "key %A *-*>* Node %i" keys nodeid         

            | DeadNodeKeyTransfer -> 
                send <! keys
            
            | ReceiveKeysFromDeadNode(keysetToAdd)->
                
                if not (keysetToAdd.IsEmpty) then
                    keys<- Set.union keys keysetToAdd
                    
              

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
       

               | SearchKey (keyid)-> 
                    
                    numberOfHops<- numberOfHops+1
                    if(keys.Contains(keyid)) then 
                        printfn "\n Node %i found key %i\n" nodeid keyid
                        // printfn "number of hopes %i" numberOfHops
                        
                    
                    else 
                        if nodeid<>keyid then
                            // printfn "\n Check For keyid in Finger table"
                            if (successor.ContainsKey(keyid)) then  
                                
                                numberOfHops<- numberOfHops+1
                                chordMap.[keyid] <! SearchKey(keyid)

                        else if nodeid = keyid then    
                            let mutable nextpossiblesuccessor = keyid + 1
                            let mutable flag = true 
                            
                            while flag do
                                
                                if (successor.ContainsKey(nextpossiblesuccessor)) then  
                                    flag<- false
                                    numberOfHops<- numberOfHops+1
                                    chordMap.[nextpossiblesuccessor] <! SearchKey(keyid)
                                else 
                                    nextpossiblesuccessor<-nextpossiblesuccessor + 1
                        
                        else 
                            let mutable nodeInFingerTable =0
                            
                            for KeyValue (k,v) in successor do  
                                if (k<=keyid) then 
                                    nodeInFingerTable<- k
                                    
                            
                            if chordMap.ContainsKey(nodeInFingerTable) then
                                numberOfHops<- numberOfHops+1
                                chordMap.[nodeInFingerTable] <! SearchKey(keyid)
                            else
                                printfn "Errorr"    


            
            //     //check whether Key exists in the key set or not
            //     //check whether the key exists in the fingertable or not
            //     printfn "\n Step 1 \n"
               
            //     let mutable fingerTableSearch = 2 
            //     printfn "\n Step 2 \n"
                
            //     if(keys.Contains(keyid)) then 
            //         printfn "Node %i Found Key** %i" nodeid keyid
            //         fingerTableSearch<-1
            //         if (fingerTableSearch = 1 ) then
            //             send<! fingerTableSearch
            //     else 
            //         printfn "\n Step 3 \n"
                   

            //         let mutable flag = false
            //         let mutable temp = -1
            //         // let mutable temp1 = -1
            //         let mutable count = 0
                   
            //         // temp<- SearchNearestNode (keyid)

            //         printfn "Successor %A" successor 
            //         for KeyValue(k,v) in successor do
            //             // if k<=keyid then 
            //             //     temp1 <- k  
            //             printfn "key %i and kryid %i" k keyid      
            //             count <- count + 1             
            //             if (k > keyid) && (not flag) then    
            //                 printf "============================>Temp chnaged to %i" k                        
            //                 temp <- k         
            //                 flag <- true
                        
            //             if not flag && (count = successor.Count) then
            //                 temp <- k
            //             // printfn "SSSTemp1------>%i" temp1
            //         printfn "SSSTemp------->%i" temp 
                                     
            //         if( (fingerTableSearch <> 1)) then
            //             numberOfHops<- numberOfHops + 1    
                                                                    
            //             // if temp-keyid>=2 then 
            //             //    printfn "\n-----if %i - %i >=2-------------\n" temp keyid
            //             //    temp<- temp1
            //             printfn "\n-----------------------------%iKey--(%itemp)" keyid temp
            //             if (temp <> -1) then
            //                 fingerTableSearch <- Async.RunSynchronously(chordMap.[temp] <? SearchInFingerTable(keyid),2000)
                        

            //         if(fingerTableSearch = 1) then                        
            //             requestedKeyFound <- true
            //             // temp <- -1
            //             // temp1<- -1
            //         else
                     
            //             if (chordMap.ContainsKey(temp))  then
            //                 printf "\nCall%i---at%i\n" keyid temp
            //                 let keySuccessor: int = Async.RunSynchronously(chordMap.[keyid] <? GetSuccessor(keyid),2000)
            //                 if(temp > keySuccessor) then
            //                     chordMap.[keySuccessor] <! SearchKey(keyid,keySuccessor)
            //                 else 
            //                     chordMap.[temp] <! SearchKey(keyid, temp)
                            
                    
            //         if (requestedKeyFound=true) then
            //             printfn "Hopes-->%i" numberOfHops 
            //             printfn "Node %i Found Key* %i" nodeid keyid
            //             send<! 1
            // | SearchNearestNode(keyid) -> 
                
            //     // if not onlyonce then
            //     //     let mutable prevnodeId = nodeid+1 
            //     //     onlyonce<- true
            //     let mutable prevnodeId = 0
            //     printfn "\n Step 4 \n"
            //     printfn "\n %A \n" successor
            //     for KeyValue(k,v) in successor do
            //         printfn "\n Step 5 \n"
                    
            //         if k<keyid then
            //             prevnodeId<-k
            //     send<! prevnodeId

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
                           
                       
            // | SearchInFingerTable (keyid)->
            //     if (keys.Contains(keyid)) then 
            //         printfn "actor-%i key-%i" nodeid keyid
            //         send<! 1
            //     else 
            //         send<! 2
            
            // | ResetNumberofHops ->    
            //     numberOfHops<-0
            // | ReturnNumberofHops ->
            //     send<! numberOfHops


            return! loop ()     
    }
    loop()


let Chordinitiate () =
    Console.WriteLine("Enter Number of Nodes and Number of Requests:") 
    let totalnodes = int fsi.CommandLineArgs.[1]
    let mutable requests= int fsi.CommandLineArgs.[2]
    num<-int(totalnodes)
    for var = 1 to num do
        chordMap <- chordMap.Add(var, spawn system (sprintf "actor%i" var) player)

    
    for var = 1 to num do
        let state = chordMap
        chordMap.[var] <! ChordMapping(chordMap,var)
  
    
    for var = 1 to num do 
        keyMap<-keyMap.Add(var)
         
    
    let mutable LowestKey = 0
    // Select which nodes to send the Keys
    let mutable randNode = 0
    let mutable onlyonce = true
    chordMap.[2] <! Callsuccessor(1)
    while not (keyMap.IsEmpty)  do
        LowestKey<- Set.minElement keyMap
       
            
        if onlyonce then
            randNode <- LowestKey
                
            onlyonce<-false               
            
        if (keyMap.Contains(LowestKey)) && chordMap.ContainsKey(randNode) then
                      
            chordMap.[randNode] <! KeyDistribute(LowestKey,randNode)            
            keyMap<-keyMap |> Set.remove LowestKey            
            onlyonce<-true    
        else                 
            randNode<- randNode+1
             
    
    let mutable deleteset = Set.empty
    let mutable upperbound = num/3
    for var = 1 to 5 do 
        let mutable nodeexists = true   
        let mutable nextnodeId = 0 
        let mutable prevnodeId = 0  
        let mutable findnext = false   
        let mutable delete =  arbitrary.Next(2,num-1)
        printfn "deleting nodee ------------> %i" delete
        while (nodeexists) do 
            if not (findnext) then
                nextnodeId<- delete + 1
                prevnodeId<- delete - 1
            
            if (chordMap.ContainsKey(nextnodeId) && chordMap.ContainsKey(prevnodeId) && chordMap.ContainsKey(delete) ) then 
                // printfn "Initiating delete for %i---p%i---n%i  " delete prevnodeId nextnodeId
                chordMap.[prevnodeId] <!  ChangeSuccessor(chordMap, nextnodeId, delete)
                let response = Async.RunSynchronously((chordMap.[delete] <? DeadNodeKeyTransfer), 2000)
                // printfn " Response-----%i---%A" delete response
                chordMap.[nextnodeId] <! ReceiveKeysFromDeadNode(response)
                if not (delete=num) then
                    let response2 = Async.RunSynchronously((chordMap.[delete] <? DeadNodeFingerTableTransfer), 2000)
                    chordMap.[nextnodeId] <! ReceiveFingerTableFromDeadNode(response2,chordMap)
                chordMap.[nextnodeId] <!  ChangePredecessor(chordMap, prevnodeId, delete)
                
                chordMap<-chordMap.Remove delete
                nodeexists<- false

            else 
                let mutable stop = false
                if (deleteset.Contains(delete) ) then
                    nodeexists<-false 
                    stop<- true
                if not (chordMap.ContainsKey(nextnodeId)) && not stop then
                    findnext<-true
                    nextnodeId<-nextnodeId+1
                    // printfn "next node %i" nextnodeId
                if not (chordMap.ContainsKey(prevnodeId)) && not stop then
                    findnext<-true
                    prevnodeId<-prevnodeId-1  
                    // printfn " previousnode %i " prevnodeId

        deleteset<-deleteset.Add(delete)
        printfn "delete set %A " deleteset
    
  
    let mutable nodeselect = 0
    let mutable covergeRatio = 0.0
    let mutable hops = 0
    let mutable count = num-1
    while not (requests = 0) do
        for KeyValue(k,v) in chordMap do
            if(k=1) then
                nodeselect<- k+1
            else 
                nodeselect<-  k

            printfn "nodeselect--->%i" nodeselect
            let mutable response3 = 0
            
            if (chordMap.ContainsKey(nodeselect)) then             
                     
                let mutable findKey = arbitrary.Next(nodeselect,num)
                // printfn "Initiating search for %i---k%i " nodeselect findKey
                chordMap.[nodeselect] <! SearchKey(findKey)
                                

               
             
        requests<-requests-1
        covergeRatio <- float(numberOfHops)/float(count)
        printfn "\n Ratio is ---%f\n"  covergeRatio      

            

    printfn "Terminate"
Chordinitiate()
    
   
    


