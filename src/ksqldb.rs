use std::io::Write;
use base64::write::EncoderWriter;
use futures_util::stream::StreamExt;
use reqwest::{ Client, header::{ HeaderMap, HeaderValue, self } };

pub struct KsqlDB
{
  pub url : String,
  pub client : Client,
}

#[ derive( Debug ) ]
pub struct Table
{
  pub names : Vec< String >,
  pub values : Vec< serde_json::Value >,
}

impl core::fmt::Display for Table
{
  fn fmt( &self, f: &mut std::fmt::Formatter<'_> ) -> std::fmt::Result
  {
    let width = self.names.len();

    // first line
    write!( f, "_" )?;
    for _ in 0..width
    {
      write!( f, "_{:_<20}_", "" )?;
    }
    writeln!( f )?;

    // Header
    write!( f, "|" )?;
    for cell in &self.names
    {
      write!( f, "{cell:<20} |" )?;
    }
    writeln!( f )?;

    // second line
    write!( f, "_" )?;
    for _ in 0..width
    {
      write!( f, "_{:_<20}_", "" )?;
    }
    writeln!( f )?;

    // Rows
    for row in &self.values
    {
      write!( f, "|" )?;
      for cell in row.as_array().unwrap()
      {
        write!( f, "{:<20} |", cell.to_string() )?
      }
      writeln!( f )?;
    }
    // last line
    write!( f, "_" )?;
    for _ in 0..width
    {
      write!( f, "_{:_<20}_", "" )?;
    }
    writeln!( f )?;
    Ok( () )
  }
}

impl KsqlDB
{
  // * Can fail on connection, it is better to return a Result
  pub fn new( url : impl Into< String >, username : impl Into< String >, password : impl Into< String > ) -> Self
  {
    let mut my_header = HeaderMap::new();

    let mut header_value = b"Basic ".to_vec();
    {
      let mut encoder = EncoderWriter::new( &mut header_value, base64::STANDARD );
      write!( encoder, "{}:", username.into() ).unwrap();
      write!( encoder, "{}", password.into() ).unwrap();
    }
    let mut header_value = HeaderValue::try_from( header_value ).unwrap();
    header_value.set_sensitive( true );

    my_header.insert( header::AUTHORIZATION , header_value );

    Self
    {
      url : url.into(),
      client : Client::builder()
      .default_headers( my_header )
      .https_only( true )
      .build().unwrap(),
    }
  }

  // *  It is better to return a Result, but for a simple example this is enough
  pub async fn select( &self, sql : impl Into< String > ) -> Table
  {
    let sql = sql.into();
    log::info!( "SELECT SQL: {sql}" );

    let mut response = self.client.post( format!( "{url}/query-stream", url = self.url ) )
    .body( format!( r#"{{ "sql" : "{sql}" }}"# ) )
    .send().await
    .unwrap()
    .bytes_stream();

    let columns = match response.next().await
    {
      Some( data ) => Ok( data.unwrap() ),
      None => Err( "Some error" ) ,
    }.unwrap();
    let mut json = serde_json::from_slice::< serde_json::Value >( &columns ).unwrap();

    let schema = json[ "columnNames" ].take();
    let names = serde_json::from_value::< Vec< String > >( schema ).unwrap_or_default()
    .into_iter()
    .map( | c | c.to_lowercase() )
    .collect();

    let values = response
    .map( | v |
    {
      serde_json::from_slice::< serde_json::Value >( &v.unwrap() ).unwrap()
    }).collect::< Vec< serde_json::Value > >().await;

    Table
    {
      names,
      values,
    }
  }
}