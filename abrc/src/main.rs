use adblock::engine::Engine;
use adblock::lists::FilterFormat;
use adblock::utils::rules_from_lists;
use clap::{App, Arg, SubCommand};
use std::fs::File;
use std::io::prelude::*;
use std::str::FromStr;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct FilterRequest {
    url: String,
    source_url: String,
    request_type: String,
}

fn main() {
    let cli = App::new("ABRaCadabra: adblock-rust CLI tool")
        .version("0.1")
        .author("Jordan Jueckstock <jjuecks@ncsu.edu>")
        .subcommand(
            SubCommand::with_name("bake")
                .about("parse and serialize a filterset from rule file[s]")
                .arg(
                    Arg::with_name("filterset")
                        .short("f")
                        .long("filterset")
                        .value_name("FILE")
                        .help("save filterset to FILE")
                        .default_value("filterset.dat"),
                )
                .arg(
                    Arg::with_name("RULE_FILE")
                        .index(1)
                        .multiple(true)
                        .min_values(1)
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("filter")
                .about("filter a stream of requests through a baked filterset")
                .arg(
                    Arg::with_name("filterset")
                        .short("f")
                        .long("filterset")
                        .value_name("FILE")
                        .help("load filterset from FILE")
                        .default_value("filterset.dat"),
                ),
        );
    let matches = cli.get_matches();

    match matches.subcommand() {
        ("bake", Some(bake)) => {
            let output_file = bake.value_of("filterset").unwrap();
            let inputs: Vec<String> = bake
                .values_of("RULE_FILE")
                .unwrap_or_default()
                .map(|s| String::from_str(s).unwrap())
                .collect();

            let rules = rules_from_lists(inputs.as_ref());
            let engine = Engine::from_rules(rules.as_ref(), FilterFormat::Standard);
            match engine.serialize() {
                Ok(blob) => match File::create(output_file) {
                    Ok(ref mut file) => {
                        file.write_all(blob.as_ref())
                            .expect("error writing serialized output");
                    }
                    Err(oops) => eprintln!("error creating output file: {0:?}", oops),
                },
                Err(oops) => eprintln!("error serializing filterset: {0:?}", oops),
            }
        }
        ("filter", Some(filter)) => {
            let input_file = filter.value_of("filterset").unwrap();
            let filterset_blob = std::fs::read(input_file).expect("unable to read filterset file");
            let mut engine = Engine::new(false);
            engine.deserialize(filterset_blob.as_ref()).expect("unable to deserialize filterset file");
            
            let request_stream = serde_json::Deserializer::from_reader(std::io::stdin()).into_iter::<FilterRequest>();
            for result in request_stream {
                match result {
                    Ok(request) => {
                        let result = engine.check_network_urls(&request.url, &request.source_url, &request.request_type);
                        println!("{0}", result.matched)
                    },
                    Err(oops) => {
                        eprintln!("error reading filter-request JSON record: {0:?}", oops)
                    }
                }
                
            }
        },
        _ => eprintln!("{0}", matches.usage()),
    }
}
