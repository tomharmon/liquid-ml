use liquid_ml::dataframe::DataFrame;
use sorer::dataframe::Data;

#[test]
fn test_from_sor() {
   let got = DataFrame::from_sor("tests/test.sor".to_string(),0, 10000);
   /*let schema = Schema::from("BIFS");
   let mut expected = DataFrame::new(&schema);
   let mut row0 = Row::new(&schema); 
   row0.set_bool(0, false);
   row0.set_int(1, 1);
   row0.set_float(2, 0.0);
   row0.set_string();*/
   assert_eq!(got.n_cols(), 4);
   assert_eq!(got.n_rows(), 2);
   assert_eq!(got.get(0,0).unwrap(), Data::Bool(false));
   assert_eq!(got.get(1,0).unwrap(), Data::Int(1));
   assert_eq!(got.get(2,0).unwrap(), Data::Float(0.0));
   assert_eq!(got.get(3,0).unwrap(), Data::String("1".to_string()));
   assert_eq!(got.get(0,1).unwrap(), Data::Bool(true));
   assert_eq!(got.get(1,1).unwrap(), Data::Int(2));
   assert_eq!(got.get(2,1).unwrap(), Data::Float(0.5));
   assert_eq!(got.get(3,1).unwrap(), Data::String("hello".to_string()));
    
}
