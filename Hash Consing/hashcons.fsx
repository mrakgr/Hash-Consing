// Hash tables for hash consing. Taken from: https://github.com/backtracking/ocaml-hashcons

type HashConsed<'a> =
    {
    hkey : int
    tag : int
    node : 'a
    }

let gentag =
    let mutable i = 0
    fun () -> i <- i+1; i

open System.Runtime.InteropServices
type WeakArray<'a>(ar) =
    new i = WeakArray(Array.init i (fun _ -> GCHandle.Alloc(null,GCHandleType.Weak)))
    member __.Length = ar.Length
    member __.Item 
        with set(i,v: 'a) = ar.[i].Target <- v
        and get(i) = ar.[i].Target :?> 'a
        
    override __.Finalize() = 
        let rec loop i =
            if i < ar.Length then
                let x = ar.[i]
                if x.IsAllocated then x.Free(); loop (i+1)
        loop 0

//type Table<'a> =
//    {
//    mutable table : WeakArray<HashConsed<'a>> []
//    mutable totsize : int // sum of the bucket sizes
//    mutable limit : int // max ratio totsize/table length
//    }
//
//let create sz =
//    let sz = max 7 sz // There is no need to check for the upper bound since it is System.Int32.MaxValue
//    {
//    table = Array.init sz <| fun _ -> WeakArray 0
//    totsize = 0
//    limit = 3
//    }
//
//let clear t =
//    for i=0 to Array.length t.table - 1 do t.table.[i] <- WeakArray 0
//    t.totsize <- 0
//    t.limit <- 3
//
