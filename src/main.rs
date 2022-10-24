use std::io::Write;

mod ksqldb;

fn input( query : impl Into< String > ) -> std::io::Result< String >
{
  print!( "{query}", query = query.into() );

  let _ = std::io::stdout().flush();
  let mut input = String::new();
  std::io::stdin().read_line( &mut input )?;

  Ok( input.trim_end_matches( "\n" ).to_owned() )
}

#[ tokio::main ]
async fn main()
{
  pretty_env_logger::init();

  log::info!( "Start program" );
  println!( "== Please enter next values ==" );

  let ( url, username, password )
  =
  (
    input( "url: " ).unwrap(),
    input( "username: " ).unwrap(),
    input( "password: " ).unwrap()
  );
  println!();
  
  let kdb = ksqldb::KsqlDB::new( url, username, password );
  log::info!( "Connected to DB" );

  loop
  {
    let input_str = input( "sql> " ).unwrap();
    // First word
    match input_str.split_ascii_whitespace().next().unwrap_or_default().to_ascii_lowercase().as_str()
    {
      "select" => println!( "{}", kdb.select( input_str ).await ),
      "exit" => std::process::exit( 0 ),
      _ => println!( "Unknown command" )
    }
  }

}