inductive Rec (α β : Type) where
  | put : α → β → Rec α β
  | del : α → Rec α β

abbrev Map (α β : Type) := α → Option β

def elem (a : Nat) : List Nat → Bool
  | [] => false
  | b :: bs => (a == b) || elem a bs

def apply {α β : Type} [DecidableEq α] (m : Map α β) : Rec α β → Map α β
  | .put k v => fun k' => if k' = k then some v else m k'
  | .del k => fun k' => if k' = k then none else m k'

def fold {α β : Type} [DecidableEq α] (rs : List (Rec α β)) : Map α β :=
  rs.foldl apply (fun _ => none)

def redo {α β : Type} (committed : List Nat) (updates : List (Nat × Rec α β)) : List (Rec α β) :=
  updates.filterMap fun u => if elem u.1 committed then some u.2 else none

def findPrev (txn : Nat) : List (Nat × Nat) → Nat
  | [] => 0
  | p :: rest => if p.1 == txn then p.2 else findPrev txn rest

def setPrev (txn lsn : Nat) : List (Nat × Nat) → List (Nat × Nat)
  | [] => [(txn, lsn)]
  | p :: rest => if p.1 == txn then (txn, lsn) :: rest else p :: setPrev txn lsn rest

structure St (α β : Type) where
  latest : Nat
  prev : List (Nat × Nat)
  committed : List Nat
  updates : List (Nat × Rec α β)

def empty {α β : Type} : St α β :=
  { latest := 0, prev := [], committed := [], updates := [] }

def accept {α β : Type} (st : St α β) (lsn txn previous : Nat) (op : Option (Rec α β)) :
    Option (St α β) :=
  if lsn == st.latest + 1 && txn != 0 && elem txn st.committed == false &&
      previous == findPrev txn st.prev && (op.isSome || findPrev txn st.prev != 0) then
    some
      { latest := lsn
        prev := setPrev txn lsn st.prev
        committed := match op with
          | some _ => st.committed
          | none => txn :: st.committed
        updates := match op with
          | some r => st.updates ++ [(txn, r)]
          | none => st.updates }
  else
    none

theorem foldl_append {γ δ : Type} (f : γ → δ → γ) (a : γ) (xs ys : List δ) :
    (xs ++ ys).foldl f a = ys.foldl f (xs.foldl f a) := by
  induction xs generalizing a with
  | nil => rfl
  | cons x xs ih => simp [List.foldl, ih]

theorem fold_snoc {α β : Type} [DecidableEq α] (rs : List (Rec α β)) (r : Rec α β) :
    fold (rs ++ [r]) = apply (fold rs) r := by
  unfold fold
  rw [foldl_append]
  rfl

theorem last_put {α β : Type} [DecidableEq α] (rs : List (Rec α β)) (k : α) (v : β) :
    fold (rs ++ [Rec.put k v]) k = some v := by
  rw [fold_snoc]
  unfold apply
  simp

theorem last_delete {α β : Type} [DecidableEq α] (rs : List (Rec α β)) (k : α) :
    fold (rs ++ [Rec.del k]) k = none := by
  rw [fold_snoc]
  unfold apply
  simp

theorem committed_put {α β : Type} [DecidableEq α] (k : α) (v : β) : fold [Rec.put k v] k = some v := by
  simpa using last_put [] k v

theorem committed_delete {α β : Type} [DecidableEq α] (k : α) (v : β) :
    fold [Rec.put k v, Rec.del k] k = none := by
  simpa using last_delete [Rec.put k v] k

theorem empty_value_present {α γ : Type} [DecidableEq α] (k : α) :
    fold [Rec.put k ([] : List γ)] k = some [] :=
  committed_put k []

theorem raw_key (k v : List UInt8) : fold [Rec.put k v] k = some v :=
  committed_put k v

theorem redo_skip {α β : Type} (txn : Nat) (r : Rec α β) (committed : List Nat)
    (h : elem txn committed = false) : redo committed [(txn, r)] = [] := by
  unfold redo
  simp [List.filterMap, h]

theorem redo_keep {α β : Type} (txn : Nat) (r : Rec α β) (committed : List Nat)
    (h : elem txn committed = true) : redo committed [(txn, r)] = [r] := by
  unfold redo
  simp [List.filterMap, h]

theorem uncommitted_invisible {α β : Type} [DecidableEq α] (txn : Nat) (r : Rec α β)
    (committed : List Nat) (k : α) (h : elem txn committed = false) :
    fold (redo committed [(txn, r)]) k = none := by
  rw [redo_skip txn r committed h]
  rfl

theorem uncommitted_keeps_prior {α β : Type} [DecidableEq α] (k : α) (v v' : β) :
    fold (redo [1] [(1, Rec.put k v), (2, Rec.put k v')]) k = some v := by
  have h : redo [1] [(1, Rec.put k v), (2, Rec.put k v')] = [Rec.put k v] := by
    unfold redo
    simp [elem.eq_1, elem.eq_2, List.filterMap]
  rw [h]
  exact committed_put k v

theorem accept_update {α β : Type} (r : Rec α β) :
    accept (empty : St α β) 1 1 0 (some r) =
      some { latest := 1, prev := [(1, 1)], committed := [], updates := [(1, r)] } := by
  unfold accept empty findPrev elem setPrev
  simp

theorem accept_commit {α β : Type} (r : Rec α β) :
    accept { latest := 1, prev := [(1, 1)], committed := [], updates := [(1, r)] } 2 1 1 none =
      some { latest := 2, prev := [(1, 2)], committed := [1], updates := [(1, r)] } := by
  unfold accept findPrev elem setPrev
  simp

theorem commit_without_update {α β : Type} : accept (empty : St α β) 1 1 0 none = none := by
  unfold accept empty findPrev elem
  simp

theorem write_log_put {α β : Type} [DecidableEq α] (k : α) (v : β) :
    (accept (empty : St α β) 1 1 0 (some (Rec.put k v))).bind
        (fun st => (accept st 2 1 1 none).map fun st => fold (redo st.committed st.updates) k) =
      some (some v) := by
  simp [accept_update, accept_commit, Option.bind, Option.map, redo, elem, List.filterMap]
  exact committed_put k v

theorem write_log_delete {α β : Type} [DecidableEq α] (k : α) :
    (accept (empty : St α β) 1 1 0 (some (Rec.del k))).bind
        (fun st => (accept st 2 1 1 none).map fun st => fold (redo st.committed st.updates) k) =
      some none := by
  simp [accept_update, accept_commit, Option.bind, Option.map, redo, elem, List.filterMap, fold,
    List.foldl, apply]
