use chrono::{DateTime, Duration, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use std::env;
use std::collections::HashMap;

const AUTH_URL: &str = "https://api.criteo.com/oauth2/token";
const STATISTICS_URL: &str = "https://api.criteo.com/2021-01/statistics/report";
const MAX_RETRY: i32 = 5;

#[derive(Serialize, Deserialize, Debug)]
struct OAuthToken {
    access_token: String,
    token_type: String,
    expires_in: i64,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct StatRequest {
    dimensions: Vec<String>,
    metrics: Vec<String>,
    format: String,
    currency: String,
    startDate: String,
    endDate: String,
}

#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug)]
struct StatResponse {
    Total: HashMap<String, String>,
    Rows: Vec<HashMap<String, String>>,
}

fn get_token(client: &reqwest::blocking::Client, client_id: &str, client_secret: &str) -> Result<reqwest::blocking::Response, reqwest::Error> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Content-Type", "application/x-www-form-urlencoded".parse().unwrap());
    headers.insert("Accept", "application/json".parse().unwrap());

    let mut payload = HashMap::new();
    payload.insert("grant_type", "client_credentials");
    payload.insert("client_id", client_id);
    payload.insert("client_secret", client_secret);

    let resp = client.post(AUTH_URL)
            .headers(headers)
            .form(&payload)
            .send()?;
    Ok(resp)
}

fn statistics_report(client: &reqwest::blocking::Client, req_date: &str, token: &str) -> Result<reqwest::blocking::Response, reqwest::Error> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("Content-Type", "application/*+json;charset=UTF-8".parse().unwrap());
    headers.insert("Authorization", format!("Bearer {}", token).parse().unwrap());
    headers.insert("Accept", "application/json".parse().unwrap());

    let dimensions = vec![
        "AdvertiserId".to_string(), "AdsetId".to_string(), "Day".to_string()
    ];
    let metrics = vec![
        "Clicks".to_string(),
        "Displays".to_string(),
        "AdvertiserCost".to_string(),
        "SalesAllClientAttribution".to_string(),
        "RevenueGeneratedAllClientAttribution".to_string(),
    ];

    let payload = StatRequest {
        dimensions: dimensions,
        metrics: metrics,
        format: "JSON".to_string(),
        currency: "JPY".to_string(),
        startDate: req_date.to_string(),
        endDate: req_date.to_string(),
    };

    let resp = client.post(STATISTICS_URL)
        .headers(headers)
        .json(&payload)
        .send()?;
    Ok(resp)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::new();
    let client_id = env::var("CRITEO_CLIENT_ID").unwrap();
    let client_secret = env::var("CRITEO_CLIENT_SECRET").unwrap();
    let mut res_token = get_token(&client, &client_id, &client_secret)
        .unwrap().text().unwrap();
    let mut token: OAuthToken = serde_json::from_str(&res_token).unwrap();

    let base_date = DateTime::parse_from_str(
        &format!("{} 00:00:00 +0000", env::var("BASE_DATE").unwrap()),
        "%Y-%m-%d %H:%M:%S %z"
    ).unwrap();
    let mut req_date = base_date;
    let lookback_window: i32 = env::var("LOOKBACK_WINDOW").unwrap().parse().unwrap();
    for _ in 1..=lookback_window {
        let mut retry_count = 0;
        req_date = req_date - Duration::days(1);
        loop {
            let extraction_timestamp: DateTime<Utc> = Utc::now();
            let res = statistics_report(&client, &req_date.format("%Y-%m-%d").to_string(), &token.access_token).unwrap();
            let status = res.status();
            if status == StatusCode::OK {
                let res_body = res.text().unwrap();
                let stat_res: StatResponse = serde_json::from_str(&res_body.trim_start_matches('\u{feff}')).unwrap();
                for rec in stat_res.Rows.iter() {
                    let mut out_rec = rec.clone();
                    out_rec.insert("extraction_timestamp".to_string(), extraction_timestamp.timestamp().to_string());
                    println!("{:?}", out_rec);
                }
                break;
            } else if status == StatusCode::UNAUTHORIZED {
                eprintln!("ERROR[{}]: {}", status.as_u16(), res.text().unwrap());
                retry_count += 1;
                if retry_count >= MAX_RETRY {
                    eprintln!("ERROR: retry limit exceeded");
                    break;
                }
                eprintln!("INFO: re-authenticate");
                res_token = get_token(&client, &client_id, &client_secret)
                    .unwrap().text().unwrap();
                token = serde_json::from_str(&res_token).unwrap();
            } else {
                retry_count += 1;
                eprintln!("ERROR[{}]: {}", status.as_u16(), res.text().unwrap());
                if retry_count >= MAX_RETRY {
                    eprintln!("ERROR: retry limit exceeded");
                    break;
                }
            }

        }
    }
    
    Ok(())
}
