def elem (a : Nat) : List Nat → Bool
  | [] => false
  | b :: bs => (a == b) || elem a bs

structure Writer (α : Type) where
  id : Nat
  size : Nat
  sealed : List Nat
  segments : List (List α)

def fresh (α : Type) (header : Nat) : Writer α :=
  { id := 0, size := header, sealed := [], segments := [[]] }

def addLast {α : Type} (buf : List α) : List (List α) → List (List α)
  | [] => [buf]
  | [seg] => [seg ++ buf]
  | seg :: mid :: rest => seg :: addLast buf (mid :: rest)

attribute [local simp] addLast.eq_1 addLast.eq_2 addLast.eq_3

def appendBuf {α : Type} (header : Nat) (w : Writer α) (buf : List α) (max : Nat) : Writer α :=
  if w.size > header ∧ w.size + buf.length > max then
    { id := w.id + 1
      size := header + buf.length
      sealed := w.sealed ++ [w.id]
      segments := w.segments ++ [buf] }
  else
    { w with size := w.size + buf.length, segments := addLast buf w.segments }

def reserve (sealed : List Nat) (id : Nat) : Option (List Nat) :=
  if elem id sealed then some (sealed.filter fun s => s != id) else none

def below (sealed : List Nat) (id : Nat) : Prop :=
  ∀ s, elem s sealed = true → s < id

theorem addLast_flatten {α : Type} (buf : List α) (segs : List (List α)) :
    (addLast buf segs).flatten = segs.flatten ++ buf := by
  induction segs with
  | nil => simp
  | cons seg rest ih =>
    cases rest with
    | nil => simp
    | cons mid rs => simp [ih]

theorem append_keeps {α : Type} (header : Nat) (w : Writer α) (buf : List α) (max : Nat) :
    (appendBuf header w buf max).segments.flatten = w.segments.flatten ++ buf := by
  unfold appendBuf
  split
  · simp
  · exact addLast_flatten buf w.segments

theorem fold_append_keeps {α : Type} (header max : Nat) (w : Writer α) (bufs : List (List α)) :
    (bufs.foldl (fun w buf => appendBuf header w buf max) w).segments.flatten =
      w.segments.flatten ++ bufs.flatten := by
  induction bufs generalizing w with
  | nil => simp
  | cons buf rest ih => simp [append_keeps, ih]

theorem fresh_absorbs {α : Type} (header : Nat) (buf : List α) (max : Nat) :
    let w := appendBuf header (fresh α header) buf max
    w.id = 0 ∧ w.sealed = [] ∧ w.segments = [buf] := by
  simp [appendBuf, fresh]

theorem rotate_whole {α : Type} (header : Nat) (w : Writer α) (buf : List α) (max : Nat)
    (hsize : w.size > header) (hover : w.size + buf.length > max) :
    let w' := appendBuf header w buf max
    w'.id = w.id + 1 ∧ w'.size = header + buf.length ∧ w'.segments = w.segments ++ [buf] ∧
      w'.sealed = w.sealed ++ [w.id] := by
  simp [appendBuf, hsize, hover]

theorem stays_when_fits {α : Type} (header : Nat) (w : Writer α) (buf : List α) (max : Nat)
    (h : w.size + buf.length ≤ max) : (appendBuf header w buf max).id = w.id := by
  unfold appendBuf
  have : ¬ (w.size > header ∧ w.size + buf.length > max) := by
    intro contra
    exact Nat.lt_irrefl (w.size + buf.length) (Nat.lt_of_le_of_lt h contra.2)
  simp [this]

theorem stays_while_empty {α : Type} (header : Nat) (w : Writer α) (buf : List α) (max : Nat)
    (h : w.size ≤ header) : (appendBuf header w buf max).id = w.id := by
  unfold appendBuf
  have : ¬ (w.size > header ∧ w.size + buf.length > max) := by
    intro contra
    exact Nat.lt_irrefl w.size (Nat.lt_of_le_of_lt h contra.1)
  simp [this]

theorem elem_append (a : Nat) (xs ys : List Nat) :
    elem a (xs ++ ys) = (elem a xs || elem a ys) := by
  induction xs with
  | nil => simp [elem]
  | cons x xs ih => simp [elem, ih, Bool.or_assoc]

theorem below_excludes (sealed : List Nat) (id : Nat) (h : below sealed id) :
    elem id sealed = false := by
  cases he : elem id sealed with
  | false => rfl
  | true => exact absurd (h id he) (Nat.lt_irrefl id)

theorem reserve_miss (sealed : List Nat) (id : Nat) (h : elem id sealed = false) :
    reserve sealed id = none := by
  simp [reserve, h]

theorem append_below {α : Type} (header : Nat) (w : Writer α) (buf : List α) (max : Nat)
    (h : below w.sealed w.id) :
    below (appendBuf header w buf max).sealed (appendBuf header w buf max).id := by
  unfold appendBuf
  split
  · intro s hs
    simp [elem_append, elem] at hs
    cases hs with
    | inl hsealed => exact Nat.lt_trans (h s hsealed) (Nat.lt_succ_self w.id)
    | inr hid =>
      have : s = w.id := by
        apply Nat.eq_of_beq_eq_true
        simpa using hid
      rw [this]
      exact Nat.lt_succ_self w.id
  · exact h

theorem active_not_reservable {α : Type} (header : Nat) (w : Writer α) (buf : List α) (max : Nat)
    (h : below w.sealed w.id) :
    reserve (appendBuf header w buf max).sealed (appendBuf header w buf max).id = none :=
  reserve_miss _ _ (below_excludes _ _ (append_below header w buf max h))
