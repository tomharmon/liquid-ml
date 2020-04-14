use chrono::{Datelike, NaiveDateTime, Timelike};
use clap::Clap;
use csv::Reader;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufWriter, Write};

/// This is a simple data processing script to convert CSV files to SoR files
/// and convert raw data to feature vectors.
#[derive(Clap)]
#[clap(version = "1.0", author = "Tom")]
struct Opts {
    /// The name of the CSV input file
    #[clap(
        short = "i",
        long = "input_file",
        default_value = "/home/tom/Downloads/spy.csv"
    )]
    input_file: String,
    /// The name of the output file
    #[clap(
        short = "o",
        long = "output_file",
        default_value = "/home/tom/Downloads/spy_processed.sor"
    )]
    output_file: String,
}

#[derive(Debug, Deserialize)]
struct Row {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
}

#[derive(Debug, Deserialize)]
struct ProcessedRow {
    /// The day of year
    day_of_year: f64,
    /// The hour of the day
    hour: f64,
    /// The minute of the hour
    minute: f64,
    /// The opening price of a candlestick
    open: f64,
    /// The highest price traded during a candlestick
    high: f64,
    /// The lowest price traded during a candlestick
    low: f64,
    /// The price of the last trade during a candlestick
    close: f64,
    /// The amount of shares traded during a candlestick
    volume: f64,
    /// The change in price from open to close of this candlestick
    delta: f64,
    /// The difference between this candlestick's high and the opening price
    /// of this candlestick
    bull_wick: f64,
    /// The difference between this candlestick's low and the closing price
    /// of this candlestick, scaled from 0-1
    bear_wick: f64,
    /// relative strength index with a period of 21 candlesticks
    rsi: f64,
    /// simple moving average with a period of 21 candlesticks
    sma: f64,
    /// average directional index with a period of 21 candlesticks
    //adx: f64,
    // TODO: maybe do multi-class classification, using an enum of `Bullish`,
    //       `Bearish`, and `Neutral`
    /// `true` if bullish (avg price of next 21 candlesticks increases compared
    /// to the closing price of this candlestick)
    label: bool,
}

/// Takes an ordered sliding window of the last 21 candlesticks (rows), the
/// row to process, and the next 21 candlesticks, and produces a new processed
/// row.
///
/// Note that according to our research, feature scaling is not needed for
/// decision trees/random forest
fn process_row(
    sliding_window: &Vec<Row>,
    row: &Row,
    forward_window: &Vec<Row>,
) -> ProcessedRow {
    assert_eq!(sliding_window.len(), 21);
    assert_eq!(forward_window.len(), 21);
    let date_time =
        NaiveDateTime::parse_from_str(&row.date, "%Y-%m-%d %H:%M:%S").unwrap();
    // not accounting for leap years
    let day_of_year = date_time.ordinal() as f64;
    let hour = date_time.hour() as f64;
    let minute = date_time.minute() as f64;

    let open = row.open;
    let high = row.high;
    let low = row.low;
    let close = row.close;

    let volume = row.volume as f64;

    let delta = row.close - row.open;
    let bull_wick = if row.close - row.open >= 0.0 {
        // this is a bullish candlestick
        row.high - row.close
    } else {
        // this is a bearish candlestick
        row.high - row.open
    };
    let bear_wick = if row.close - row.open >= 0.0 {
        // this is a bullish candlestick
        row.open - row.low
    } else {
        // this is a bearish candlestick
        row.close - row.low
    };

    // rsi calculation
    let sliding_window_delta: Vec<_> =
        sliding_window.iter().map(|r| r.close - r.open).collect();
    let avg_gain = sliding_window_delta.iter().fold(0.0, |acc, &d| {
        if d >= 0.0 {
            acc + d
        } else {
            acc
        }
    }) / 21.0;
    let avg_loss = sliding_window_delta.iter().fold(0.0, |acc, &d| {
        if d <= 0.0 {
            acc + d.abs()
        } else {
            acc
        }
    }) / 21.0;
    let rs = avg_gain / avg_loss;
    let rsi = 100.0 - 100.0 / (1.0 + rs);

    // sma calculation
    let sma =
        sliding_window.iter().fold(0.0, |acc, r| acc + avg_ohlc(r)) / 21.0;

    // TODO: adx calculation

    // label calculation
    let forward_delta =
        avg_ohlc(forward_window.last().unwrap()) - avg_ohlc(row);
    let label = forward_delta > 0.0;

    ProcessedRow {
        day_of_year,
        hour,
        minute,
        open,
        high,
        low,
        close,
        volume,
        delta,
        bull_wick,
        bear_wick,
        rsi,
        sma,
        label,
    }
}

// takes a row and returns the average of the open, high, low, and close
fn avg_ohlc(r: &Row) -> f64 {
    (r.open + r.high + r.low + r.close) / 4.0
}

fn main() {
    let opts: Opts = Opts::parse();
    let mut csv_reader = Reader::from_path(&opts.input_file).unwrap();
    // 1. Aggregate the first 21 and next rows so we can calculate `rsi`,
    //    `sma`, `adx`, and the `label`
    //
    // # RSI - Relative Strength Index
    // - RSI = 100 - 100 / (1 + RS)
    // - RS = Average gain of last 14 trading days /
    //        Average loss of last 14 trading days
    // - https://www.macroption.com/rsi-calculation/
    //
    //  # SMA - Simple Moving Average
    //  - SMA = ( Sum ( Price, n ) ) / n
    //  - n = number of bars in a period, in this case we chose 21
    //  - https://www.tradingtechnologies.com/xtrader-help/x-study/technical-indicator-definitions/simple-moving-average-sma/
    //
    //  # ADX - Average Directional Index
    //  - TODO
    //  - https://www.investopedia.com/terms/a/adx.asp

    // gather the first 21 rows
    let mut sliding_window: Vec<Row> = csv_reader
        .deserialize()
        .take(21)
        .map(|res| res.unwrap())
        .collect();
    // oldest candlesticks are at the back of the sliding window
    sliding_window.reverse();

    // gather the next 21 rows
    let mut csv_reader = Reader::from_path(&opts.input_file).unwrap();
    // candlesticks closest to current candlestick at the front, candlesticks
    // furthest in the future at the back
    let mut forward_window: Vec<Row> = csv_reader
        .deserialize()
        .skip(21)
        .take(21)
        .map(|res| res.unwrap())
        .collect();

    // 2. Process the rest of the data normally
    let mut csv_reader = Reader::from_path(&opts.input_file).unwrap();
    let mut peek_iter = csv_reader.deserialize().skip(21 * 2).peekable();
    let mut csv_reader = Reader::from_path(&opts.input_file).unwrap();
    let iter = csv_reader.deserialize().skip(21);
    let mut results = Vec::new();

    for result in iter {
        let row: Row = result.unwrap();
        // process the row and add the result to our list of processed rows
        let processed = process_row(&sliding_window, &row, &forward_window);
        results.push(processed);
        // remove the oldest row in the sliding window
        sliding_window.pop();
        // add this row to the sliding window
        sliding_window.insert(0, row);
        // peek the next row to add to our forward_window or break if we are
        // done
        if peek_iter.peek().is_none() {
            break;
        } else {
            forward_window.remove(0);
            forward_window.push(peek_iter.next().unwrap().unwrap());
        }
    }

    // 4. Save the processed results in a `SoR` file
    let mut writer = BufWriter::new(File::create(&opts.output_file).unwrap());
    for processed_row in results {
        writer
            .write(
                format!(
                    "<{}><{}><{}><{}><{}><{}><{}><{}><{}><{}><{}><{}><{}><{}>\n",
                    processed_row.day_of_year,
                    processed_row.hour,
                    processed_row.minute,
                    processed_row.open,
                    processed_row.high,
                    processed_row.low,
                    processed_row.close,
                    processed_row.volume,
                    processed_row.delta,
                    processed_row.bull_wick,
                    processed_row.bear_wick,
                    processed_row.rsi,
                    processed_row.sma,
                    processed_row.label
                )
                .as_bytes(),
            )
            .unwrap();
    }
}
