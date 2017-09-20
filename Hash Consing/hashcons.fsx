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
type WeakArray<'a>(ar) = // Partly based on: https://www.codeproject.com/Articles/43042/WeakReferences-GCHandles-and-WeakArrays
    new i = WeakArray(Array.init i (fun _ -> GCHandle.Alloc(null,GCHandleType.Weak)))
    member __.Length = ar.Length
    member __.Item 
        with set i (v: 'a) = ar.[i].Target <- v
        and get(i) = 
            match ar.[i].Target with
            | null -> None
            | v -> Some (v :?> 'a)
        
    override __.Finalize() = 
        let rec loop i =
            if i < ar.Length then
                let x = ar.[i]
                if x.IsAllocated then x.Free(); loop (i+1)
        loop 0

let weak_length (x: WeakArray<_>) = x.Length

type Table<'a> =
    {
    mutable table : WeakArray<HashConsed<'a>> []
    mutable totsize : int // sum of the bucket sizes
    mutable limit : int // max ratio totsize/table length
    }

let create sz =
    let sz = max 7 sz // There is no need to check for the upper bound since it is System.Int32.MaxValue
    {
    table = Array.init sz <| fun _ -> WeakArray 0
    totsize = 0
    limit = 3
    }

let clear t =
    for i=0 to Array.length t.table - 1 do t.table.[i] <- WeakArray 0
    t.totsize <- 0
    t.limit <- 3

let foldr f t init =
    let rec fold_bucket i b accu =
        if i >= weak_length b then accu else
            match b.[i] with
            | None -> fold_bucket (i+1) b accu
            | Some v -> fold_bucket (i+1) b (f v accu)
    Array.foldBack (fold_bucket 0) t.table init
        
let iter f t =
    let rec iter_bucket i b =
        if i >= weak_length b then () else
            match b.[i] with
            | None -> iter_bucket (i+1) b
            | Some v -> f v; iter_bucket (i+1) b
    Array.iter (iter_bucket 0) t.table

