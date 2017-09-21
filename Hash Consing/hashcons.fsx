// Hash tables for hash consing. Taken from: https://github.com/backtracking/ocaml-hashcons
open System
open System.Runtime.InteropServices

type HashConsed<'a> =
    {
    hkey : int
    tag : int
    node : 'a
    }

let gentag =
    let mutable i = 0
    fun () -> i <- i+1; i

type WeakArray<'a>(ar) = // Partly based on: https://www.codeproject.com/Articles/43042/WeakReferences-GCHandles-and-WeakArrays
    new i = WeakArray(Array.init i (fun _ -> GCHandle.Alloc(null,GCHandleType.Weak)))
    member __.GCHandleArray = ar
    member __.Length = ar.Length
    member __.Item 
        with set i (v: 'a) = ar.[i].Target <- v
        and get(i) = 
            match ar.[i].Target with
            | null -> None
            | v -> Some (v :?> 'a)
    member t.GetCopy i =
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
let weak_check (b: WeakArray<_>) i = match b.[i] with None -> false | Some _ -> true
let weak_create (sz: int) = WeakArray sz
let weak_gchandlearray (x: WeakArray<_>) = x.GCHandleArray
let weak_blit a ao b bo sz = Array.blit (weak_gchandlearray a) ao (weak_gchandlearray b) bo sz

type Table<'a> =
    {
    mutable table : WeakArray<HashConsed<'a>> []
    mutable totsize : int // sum of the bucket sizes
    mutable limit : int // max ratio totsize/table length
    }

let inline in_range a b x = max a (min b x)
let max_array_length = Int32.MaxValue
let create sz =
    let sz = max 7 sz // There is no need to check for the upper bound since it is System.Int32.MaxValue for .NET.
    let empty_bucket = WeakArray 0
    {
    table = Array.create sz empty_bucket // This sharing is not a mistake. Empty arrays get replaced in the `add` function.
    totsize = 0
    limit = 3
    }

let clear t =
    let empty_bucket = WeakArray 0
    for i=0 to Array.length t.table - 1 do t.table.[i] <- empty_bucket // This sharing is not a mistake.
    t.totsize <- 0
    t.limit <- 3

let foldBack f t init =
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

let count t =
    let rec count_bucket i b accu =
        if i >= weak_length b then accu else
            if weak_check b i then accu+1 else accu
            |> count_bucket (i+1) b
    Array.foldBack (count_bucket 0) t.table 0

/// Note: Does not check for overflow.
let next_sz n = min (3*n/2+3) max_array_length

let rec resize t =
    let oldlen = Array.length t.table
    let newlen = next_sz oldlen
    if newlen > oldlen then
        let newt = create newlen
        newt.limit <- t.limit + 100 // prevent resizing of newt
        iter (fun d -> add newt d) t
        t.table <- newt.table
        t.limit <- t.limit+2
and add t d =
    let index = d.hkey % (Array.length t.table)
    let bucket = t.table.[index]
    let sz = weak_length bucket
    let rec loop i =
        if i >= sz then
            let newsz = min (sz + 3) max_array_length
            if newsz <= sz then failwith "Hashcons.make: hash bucket cannot grow more"
            let newbucket = weak_create newsz
            weak_blit bucket 0 newbucket 0 sz
            newbucket.[i] <- d
            t.table.[index] <- newbucket
            t.totsize <- t.totsize + (newsz - sz)
            if t.totsize > t.limit * Array.length t.table then resize t
        else
            if weak_check bucket i then loop (i+1)
            else bucket.[i] <- d
    loop 0

let hashcons t d =
    let hkey = hash d &&& Int32.MaxValue // Zeroes out the sign bit. Is unnecessary in the Ocaml version.
    let index = hkey % (Array.length t.table)
    let bucket = t.table.[index]
    let sz = weak_length bucket
    let rec loop i =
        if i >= sz then
            let hnode = { hkey = hkey; tag = gentag(); node = d}
            add t hnode
            hnode
        else
            // TODO: The OCaml version uses the get_copy here, while this is just a regular fetch.
            // Figure out how that affect the semantics of the thing.
            match bucket.[i] with
            | Some v when v.node = d -> v
            | _ -> loop (i+1)
    loop 0

type HashStats =
    {
    table_length: int
    number_of_entries: int
    sum_of_bucket_lengths: int
    smallest_bucket_length: int
    median_bucket_length: int
    biggest_bucket_length: int
    }

let stats t =
    let len = Array.length t.table
    let lens = Array.map weak_length t.table
    Array.sortInPlace lens
    let totlen = Array.sum lens
    { 
    table_length = len; number_of_entries = count t; sum_of_bucket_lengths = totlen
    smallest_bucket_length = lens.[0]; median_bucket_length = lens.[len/2]; biggest_bucket_length = Array.last lens 
    }

module Map = 
    type ConsedMap<'a,'b> =
        | Empty
        | Leaf of HashConsed<'a> * 'b
        | Branch of int * int * ConsedMap<'a,'b> * ConsedMap<'a,'b>

    let zero_bit k m = (k &&& m) = 0

    let rec mem k = function
        | Empty -> false
        | Leaf(j,_) -> k.tag = j.tag
        | Branch(_, m, l, r) -> mem k (if zero_bit k.tag m then l else r)

    let rec find k = function
        | Empty -> failwith "Not found."
        | Leaf(j,x) -> if k.tag = j.tag then x else failwith "Not found."
        | Branch(_, m, l, r) -> mem k (if zero_bit k.tag m then l else r)

    let lowest_bit x = x &&& (-x)
    let branching_bit p0 p1 = lowest_bit (p0 ^^^ p1)
    let mask p m = p &&& (m-1)

    let join (p0,t0,p1,t1) =
        let m = branching_bit p0 p1
        if zero_bit p0 m then
            Branch (mask p0 m, m, t0, t1)
        else
            Branch (mask p0 m, m, t1, t0)

    let match_prefix k p m = (mask k m) = p

    let add k x t =
        let rec ins = function
            | Empty -> Leaf (k,x)
            | Leaf(j,_) as t ->
                if j.tag = k.tag then Leaf(k,x)
                else join (k.tag, Leaf(k,x), j.tag, t)
            | Branch (p,m,t0,t1) as t ->
                if match_prefix k.tag p m then
                    if zero_bit k.tag m then Branch (p, m, ins t0, t1)
                    else Branch (p, m, t0, ins t1)
                else join (k.tag, Leaf (k,x), p, t)
        ins t


    let branch = function
        | (_,_,Empty,t) -> t
        | (_,_,t,Empty) -> t
        | (p,m,t0,t1)   -> Branch (p,m,t0,t1)

    let remove k t =
        let rec rmv = function
            | Empty -> Empty
            | Leaf (j,_) as t -> if k.tag = j.tag then Empty else t
            | Branch (p,m,t0,t1) as t ->
                if match_prefix k.tag p m then
                    if zero_bit k.tag m then branch (p, m, rmv t0, t1)
                    else branch (p, m, t0, rmv t1)
                else t
        rmv t

    let rec iter f = function
        | Empty -> ()
        | Leaf (k,x) -> f k x
        | Branch (_,_,t0,t1) -> iter f t0; iter f t1

    let rec map f = function
        | Empty -> Empty
        | Leaf (k,x) -> Leaf (k, f x)
        | Branch (p,m,t0,t1) -> Branch (p, m, map f t0, map f t1)

    let rec mapi f = function
        | Empty -> Empty
        | Leaf (k,x) -> Leaf (k, f k x)
        | Branch (p,m,t0,t1) -> Branch (p, m, mapi f t0, mapi f t1)

    let rec fold f s accu = 
        match s with
        | Empty -> accu
        | Leaf (k,x) -> f k x accu
        | Branch (_,_,t0,t1) -> fold f t0 (fold f t1 accu)