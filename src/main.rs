use clap::{App, Arg};
use futures::future::join_all;
use hyper::body::HttpBody as _;
use hyper::{Client, Uri};
use hyper_tls::HttpsConnector;
use rand::distributions::{Distribution, Uniform};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
    /// used to tabulate statists
    name: String,
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
    pub name: String,
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
            match res {
                Ok(req) => requests.push(req),
                Err(err) => return Err(err),
            }
        }
        Ok(Self {
            proportion: request.proportion,
            name: request.name.clone(),
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
            match res {
                Ok(req) => requests.push(req),
                Err(err) => return Err(err),
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
async fn run_request_group(group: &RequestGroup) -> HashMap<String, Vec<Vec<RequestStatus>>> {
    let requests = group
        .requests
        .iter()
        .map(|request| vec![(request.requests.clone(), request.name.clone()); request.proportion])
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
    let mut names = vec![];
    let mut delay_times = join_all(times.map(|(starting_delay, index)| {
        names.push(requests[index].1.clone());
        run_request_chain(starting_delay, &requests[index].0)
    }))
    .await;
    let mut status_out = HashMap::new();
    for (idx, delay) in delay_times.drain(..).enumerate() {
        if !status_out.contains_key(&names[idx]) {
            status_out.insert(names[idx].clone(), vec![]);
        }
        status_out.get_mut(&names[idx]).unwrap().push(delay);
    }
    return status_out;
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
        while resp.body_mut().data().await.is_some() {}
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
struct StatisticsClient {
    pub name: String,
    pub average_total_load_time: Duration,
    pub standard_deviation: Duration,
}
struct Statistics {
    pub clients: Vec<StatisticsClient>,
}
impl std::fmt::Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{0:<10} | {1:<20} | {2:<10}",
            "name", "avg load time (s)", "std dev (s)"
        )?;
        for c in self.clients.iter() {
            write!(
                f,
                "\n{0:<10} | {1:<20} | {2:<10}",
                c.name,
                c.average_total_load_time.as_secs_f64(),
                c.standard_deviation.as_secs_f64()
            )?
        }
        Ok(())
    }
}
fn get_stat(data: &HashMap<String, Vec<Vec<RequestStatus>>>) -> Statistics {
    let get_chain_status = |s: &[RequestStatus]| {
        let mut duration = Duration::default();
        for status in s.iter() {
            match status {
                RequestStatus::Sucess { delay, .. } => duration += *delay,
                RequestStatus::HttpParseError => return RequestStatus::HttpParseError,
                RequestStatus::Timeout => return RequestStatus::Timeout,
                RequestStatus::Other => return RequestStatus::Other,
            }
        }
        return RequestStatus::Sucess {
            delay: duration,
            url: String::new(),
        };
    };
    Statistics {
        clients: data
            .iter()
            .map(|(name, requests)| {
                let num_sucess = requests
                    .iter()
                    .map(|r_chain| get_chain_status(r_chain))
                    .count();
                let mean = requests
                    .iter()
                    .map(|r_chain| get_chain_status(r_chain))
                    .filter_map(|req| match req {
                        RequestStatus::Sucess { delay, .. } => Some(delay),
                        _ => None,
                    })
                    .fold(Duration::default(), |acc, req| acc + req)
                    / num_sucess as u32;
                let standard_deviation: f64 = (requests
                    .iter()
                    .map(|r_chain| get_chain_status(r_chain))
                    .filter_map(|req| match req {
                        RequestStatus::Sucess { delay, .. } => Some(delay),
                        _ => None,
                    })
                    .map(|r| (r.as_secs_f64() - mean.as_secs_f64()).powi(2))
                    .sum::<f64>()
                    / (num_sucess as f64))
                    .sqrt();
                StatisticsClient {
                    name: name.clone(),
                    average_total_load_time: mean,
                    standard_deviation: Duration::from_secs_f64(standard_deviation),
                }
            })
            .collect(),
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
                .possible_value("stat")
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
            "stat" => format!("{}", get_stat(&status)),
            _ => String::new(),
        }
    );
    Ok(())
}
