//! A visitor invoked by a `Row`.

/// A field visitor invoked by a `Row`.
pub trait Fielder {
    /// Must be called before visiting a row
    fn start(&mut self, starting_row_index: usize);

    /// Called for fields of type `bool` with the value of the field
    fn visit_bool(&mut self, b: bool);

    /// Called for fields of type `float` with the value of the field
    fn visit_float(&mut self, f: f64);

    /// Called for fields of type `int` with the value of the field
    fn visit_int(&mut self, i: i64);

    /// Called for fields of type `String` with the value of the field
    fn visit_string(&mut self, s: &String);

    /// Called for fields where the value of the field is missing
    fn visit_null(&mut self);

    /// Called when all fields have been seen
    fn done(&mut self);
}
