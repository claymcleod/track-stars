use std::env;
use std::path::PathBuf;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use clap::Parser;
use clap::ValueHint;
use eyre::Context;
use eyre::Result;
use eyre::bail;
use reqwest::header::AUTHORIZATION;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::USER_AGENT;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::sleep;

/// A query to send off to the GitHub GraphQL API.
#[derive(Serialize)]
struct Query<'a> {
    /// The query.
    query: &'a str,
}

/// A star event for a repository.
#[derive(Debug, Deserialize)]
struct Star {
    /// The time the repository was starred.
    #[serde(rename = "starredAt")]
    starred_at: DateTime<Utc>,

    /// The user that starred the repo.
    node: User,
}

/// A followers/following count.
#[derive(Debug, Deserialize)]
struct FollowCount {
    /// The total number of followers/following.
    #[serde(rename = "totalCount")]
    total_count: usize,
}

/// A Github user.
#[derive(Debug, Deserialize)]
struct User {
    /// The user's username.
    login: String,

    /// The user's email.
    email: Option<String>,

    /// The location of the user.
    location: Option<String>,

    /// The number of followers the individual has.
    followers: FollowCount,

    /// The number people the individual is following.
    following: FollowCount,

    /// If the user marked themselves as hireable.
    #[serde(rename = "isHireable")]
    is_hireable: bool,
}

/// The entire GraphQL response.
#[derive(Debug, Deserialize)]
struct Response {
    /// The response data.
    data: ResponseData,
}

/// The response data.
#[derive(Debug, Deserialize)]
struct ResponseData {
    /// The repository.
    repository: Repository,
}

/// The repository in the GraphQL response.
#[derive(Debug, Deserialize)]
struct Repository {
    /// The stargazers for that repository.
    stargazers: Stargazers,
}

/// The stargazers for a GitHub repository.
#[derive(Debug, Deserialize)]
struct Stargazers {
    /// The edges in the stars graph.
    edges: Vec<Star>,

    /// The pagination information.
    #[serde(rename = "pageInfo")]
    page_info: PageInfo,
}

/// The pagination information.
#[derive(Debug, Deserialize)]
struct PageInfo {
    /// Whether or not a next page exists.
    #[serde(rename = "hasNextPage")]
    has_next_page: bool,

    /// The last cursor.
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

/// A row in the final CSV table.
#[derive(Debug, Serialize)]
struct Row {
    /// The date the star was given.
    date: DateTime<Utc>,

    /// The username of the individual giving the star.
    username: String,

    /// The individual's email.
    email: Option<String>,

    /// The organization of the individual.
    location: Option<String>,

    /// The number of followers the individual has.
    followers: usize,

    /// The number of people the individual is following.
    following: usize,

    /// Whether or not the individual is hireable.
    hireable: bool,
}

/// Writes a list of stargazers to a CSV.
#[derive(Debug, Parser)]
pub struct Args {
    /// The organization or owner of the repository.
    owner: String,

    /// The repository.
    repository: String,

    /// The path to the output file.
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    path: Option<PathBuf>,
}

/// Fetches a single page of results for the stargazers.
async fn fetch_page(
    args: &Args,
    token: &str,
    count: usize,
    after: Option<String>,
) -> Result<Response> {
    let after_clause = match after {
        Some(n) => format!(r#", after: "{}""#, n),
        None => String::new(),
    };

    let query = format!(
        r#"
        {{
            repository(owner: "{}", name: "{}") {{
                stargazers(first: {}{}) {{
                    edges {{
                        starredAt,
                        node {{
                          name,
                          email,
                          login,
                          location,
                          followers {{
                            totalCount
                          }},
                          following {{
                            totalCount
                          }},
                          isHireable
                        }}
                    }}
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                }}
            }}
        }}
        "#,
        args.owner, args.repository, count, after_clause
    );

    let client = reqwest::Client::new();
    let mut headers = HeaderMap::new();

    headers.insert(USER_AGENT, HeaderValue::from_str("star-tracker/v0")?);
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token))?,
    );

    sleep(Duration::from_secs(3)).await;

    let request = client
        .post("https://api.github.com/graphql")
        .headers(headers)
        .json(&Query { query: &query });

    let response = request.send().await?;

    if response.status().is_success() {
        response
            .json()
            .await
            .context("serializing GitHub response to JSON")
    } else {
        bail!(
            "failed to fetch data from GitHub GraphQL API: {:?}",
            response.status()
        )
    }
}

async fn fetch(args: &Args, token: &str) -> Result<Vec<Star>> {
    let mut results = Vec::new();
    let mut cursor = None;
    let mut has_next_page = true;
    let mut users = 0;

    while has_next_page {
        let response = fetch_page(args, token, 100, cursor)
            .await
            .context("querying GitHub")?;

        users += response.data.repository.stargazers.edges.len();
        eprintln!("Users: {users}");

        results.extend(response.data.repository.stargazers.edges);
        cursor = response.data.repository.stargazers.page_info.end_cursor;
        has_next_page = response.data.repository.stargazers.page_info.has_next_page;
    }

    Ok(results)
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install().context("installing color eyre")?;

    let args = Args::parse();
    let token = env::var("GH_TOKEN").expect("github token to be present");

    let stars = fetch(&args, &token).await.context("fetching stargazers")?;

    let path = args
        .path
        .unwrap_or_else(|| format!("{}-{}-stargazers.csv", args.owner, args.repository).into());

    eprintln!("writing {} records to {}.", stars.len(), path.display());

    let mut writer = csv::Writer::from_path(&path)
        .with_context(|| format!("opening output file path at {}", path.display()))?;

    for star in stars {
        writer
            .serialize(Row {
                date: star.starred_at,
                username: star.node.login,
                email: star.node.email,
                location: star.node.location,
                followers: star.node.followers.total_count,
                following: star.node.following.total_count,
                hireable: star.node.is_hireable,
            })
            .context("writing star record")?;
    }

    Ok(())
}
