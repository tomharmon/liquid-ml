use csv::Reader;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Debug, Deserialize)]
struct Row {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: usize,
    open_int: usize,
}

#[derive(Debug, Deserialize)]
struct ProcessedRow {
    ticker: String,
    year: usize, // scaling?
    date: f64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    /// relative strength index, 2 week period
    rsi: f64,
    /// simple moving average, 2 week period
    sma: f64,
    /// exponential moving average, 2 week period
    ema: f64,
    /// average directional index, 2 week period
    adx: f64,
    /// bullish/bearish
    label: bool,
}

fn main() {
    // Open the CSV file, it's in the format of:
    // Date,Open,High,Low,Close,Volume,OpenInt
    // we'll convert it into this format
    // :::
    let mut csv_reader = Reader::from_path("file").unwrap();
    for result in csv_reader.deserialize() {
        let row: Row = result.unwrap();
        // do some math
        unimplemented!()
    }
}
