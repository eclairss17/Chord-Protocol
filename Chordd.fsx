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
    | KeyDistribute of int
    | FingerTable of Map<int, IActorRef>*int

let player(mailbox: Actor<_>) =
    let mutable successor = Map.empty
    let mutable predecessor = Map.empty
    let mutable keys = Set.empty
    let mutable totalplayers = 0
    let mutable nodeid= 0
    let rec loop () = 
        actor {
            let! msg = mailbox.Receive()
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
                
            | KeyDistribute( keyid) -> 
                keys<-keys.Add(keyid)

            | FingerTable(chordMap,totalnodes) -> 
                let mutable flag = true 
                let mutable onlyonce = true 
                for i=0 to totalnodes do
                    let mutable nextnode = (2.0)**float(i) + float(nodeid)
                    if(nextnode <float(totalnodes)) && flag then
                        successor<-successor.Add(int(nextnode),chordMap.[int(nextnode)])
                        printfn "Finger Table for %i has a new entry %i" nodeid (int(nextnode))
                    else 
                        flag <- false
                        if (onlyonce) then 
                            successor<-successor.Add(totalnodes,chordMap.[totalnodes])
                            printfn "Finger Table for %i has last entry %i" nodeid totalnodes
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
    printfn " All chords initially mapped"
    
    //Select the amount of Keys in circulation 
    let amount = arbitrary.Next((2*num/3),(num+1))
    for var = 1 to amount do 
        keyMap<-keyMap.Add(var)
    //Select which nodes to send the Keys
    while not (keyMap.IsEmpty) do
        let randNode = arbitrary.Next(1,(num+1))
        let randKey = arbitrary.Next(1,keyMap.Count+1)
        if(keyMap.Contains(randKey)) then
            printfn " Sending Key %i to %i " randKey randNode
            chordMap.[randNode] <! KeyDistribute(randKey)
            keyMap<-keyMap |> Set.remove randKey
    printfn "Terminate"
        
Chordinitiate(10)
    

