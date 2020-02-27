/// A field visitor invoked by `Row`.
trait Fielder {
    /// Must be called before visiting a row, the argument is the row offset
    /// in the dataframe.
    fn start(starting_row_index: usize);

    /// Called for fields of type bool with the value of the field
    fn accept(b: bool);
    /// Called for fields of type float with the value of the field
    fn accept(f: f64);
    /// Called for fields of type int with the value of the field
    fn accept(i: i64);
    /// Called for fields of type String with the value of the field
    fn accept(s: &String);

    /// Called when all fields have been seen
    fn done();
}
