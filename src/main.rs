use clap::{App, Arg};
use futures::future::join_all;
use hyper::body::HttpBody as _;
use hyper::{Client, Uri};
use hyper_tls::HttpsConnector;
use rand::distributions::{Distribution, Uniform};
use serde::{Deserialize, Serialize};
use std::{
    convert::{TryFrom, TryInto},
    time::{Duration, Instant},
};
use tokio::{fs::File, io::AsyncReadExt, time::sleep};

struct RequestGroup {
    requests: Vec<RankedRequest>,
    /// Total number of requests to send
    number_of_requests: u32,
    /// Duration of time over which to smear requests
    duration: Duration,
}
struct RankedRequest {
    proportion: usize,
    requests: Vec<Request>,
}

#[derive(Clone, Debug)]
struct Request {
    pub uri: Uri,
    pub delay: Duration,
}
/// Deserializble Request
#[derive(Clone, Debug, Deserialize)]
pub struct DRequest {
    pub url: String,
    pub delay_s: f64,
}
#[derive(Clone, Debug, Deserialize)]
pub struct DRankedRequest {
    pub proportion: usize,
    pub requests: Vec<DRequest>,
}
#[derive(Clone, Debug, Deserialize)]
pub struct DRequestGroup {
    pub requests: Vec<DRankedRequest>,
    /// Total number of requests to send
    pub number_of_requests: u32,
    /// Duration of time over which to smear requests
    pub duration_s: f64,
}
impl TryFrom<&DRankedRequest> for RankedRequest {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn try_from(request: &DRankedRequest) -> Result<Self, Self::Error> {
        let mut requests = vec![];
        for r in request.requests.iter() {
            let res: Result<Request, _> = r.try_into();
            if res.is_ok() {
                requests.push(res.unwrap());
            } else {
                return Err(res.err().unwrap());
            }
        }
        Ok(Self {
            proportion: request.proportion,
            requests,
        })
    }
}
impl TryFrom<DRequestGroup> for RequestGroup {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn try_from(request: DRequestGroup) -> Result<Self, Self::Error> {
        let mut requests = vec![];
        for r in request.requests.iter() {
            let res: Result<RankedRequest, _> = r.try_into();
            if res.is_ok() {
                requests.push(res.unwrap());
            } else {
                return Err(res.err().unwrap());
            }
        }
        Ok(Self {
            requests,
            duration: Duration::from_secs_f64(request.duration_s),
            number_of_requests: request.number_of_requests,
        })
    }
}

impl TryFrom<&DRequest> for Request {
    type Error = Box<dyn std::error::Error + Send + Sync>;
    fn try_from(request: &DRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            uri: request.url.parse()?,
            delay: Duration::from_secs_f64(request.delay_s),
        })
    }
}
#[derive(Clone, Debug, Serialize)]
enum RequestStatus {
    Sucess { delay: Duration, url: String },
    HttpParseError,
    Timeout,
    Other,
}
async fn run_request_group(group: &RequestGroup) -> Vec<Vec<RequestStatus>> {
    let requests = group
        .requests
        .iter()
        .map(|request| vec![request.requests.clone(); request.proportion])
        .flatten()
        .collect::<Vec<_>>();
    assert_ne!(requests.len(), 0);
    let mut rng = rand::thread_rng();
    let distribution = Uniform::from(0..requests.len());
    let times = (0..group.number_of_requests).map(|_| {
        (
            Duration::from_secs_f64(rand::random::<f64>() * group.duration.as_secs_f64()),
            distribution.sample(&mut rng),
        )
    });
    let delay_times = join_all(
        times.map(|(starting_delay, index)| run_request_chain(starting_delay, &requests[index])),
    )
    .await;
    delay_times
}
async fn run_request_chain(starting_delay: Duration, requests: &[Request]) -> Vec<RequestStatus> {
    sleep(starting_delay).await;
    join_all(requests.iter().map(|req| run_request(req))).await
}
async fn run_request(request: &Request) -> RequestStatus {
    let delay = get_url(request.uri.clone()).await;
    sleep(request.delay).await;
    delay
}
/// Gets from url and returns time
async fn get_url(uri: Uri) -> RequestStatus {
    let https = HttpsConnector::new();
    let client = Client::builder().build::<_, hyper::Body>(https);
    let now = Instant::now();
    let status = client.get(uri.clone()).await;

    if status.is_ok() {
        let mut resp = status.unwrap();
        while let Some(_) = resp.body_mut().data().await {}
        RequestStatus::Sucess {
            url: format!("{}", uri),
            delay: now.elapsed(),
        }
    } else {
        let error = status.err().unwrap();
        if error.is_parse() {
            RequestStatus::HttpParseError
        } else if error.is_timeout() {
            RequestStatus::Timeout
        } else {
            RequestStatus::Other
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let matches = App::new("Amawk")
        .version("0.1")
        .author("Nicholas Alexeev")
        .arg(
            Arg::with_name("config")
                .short("c")
                .help("YML flile that speficfies tests to run")
                .default_value("config.yml"),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .help("Specifies output Format")
                .possible_value("json")
                .default_value("json"),
        )
        .get_matches();
    let config_file_path = matches.value_of("config").unwrap();
    let mut file = File::open(config_file_path).await?;
    let mut file_contents = String::new();
    file.read_to_string(&mut file_contents).await?;
    let parsed_config: DRequestGroup = serde_yaml::from_str(&file_contents)?;
    let request_group: RequestGroup = parsed_config.try_into().expect("Failed to Parse");
    let status = run_request_group(&request_group).await;
    println!(
        "{}",
        match matches.value_of("output").unwrap() {
            "json" => serde_json::to_string(&status).expect("failed to parse into valid json"),
            _ => String::new(),
        }
    );
    Ok(())
}
