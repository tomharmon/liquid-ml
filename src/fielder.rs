/// A field visitor invoked by `Row`.
pub trait Fielder {
    /// Must be called before visiting a row, the argument is the row offset
    /// in the dataframe.
    fn start(&self, starting_row_index: usize);

    /// Called for fields of type bool with the value of the field
    fn visit_bool(&self, b: bool);

    /// Called for fields of type float with the value of the field
    fn visit_float(&self, f: f64);

    /// Called for fields of type int with the value of the field
    fn visit_int(&self, i: i64);

    /// Called for fields of type String with the value of the field
    fn visit_string(&self, s: &String);

    /// Called for fields of type String with the value of the field
    fn visit_null(&self);

    /// Called when all fields have been seen
    fn done(&self);
}
