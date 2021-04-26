use std::env;
use std::fmt;
use std::path::PathBuf;

use futures::{SinkExt, StreamExt, TryStreamExt};

use serde::de::DeserializeOwned;
use serde_derive::Deserialize;

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_util::codec::{BytesCodec, FramedWrite};

#[derive(Debug, Deserialize)]
#[serde(bound = "T: fmt::Debug + DeserializeOwned")]
pub struct ResponseEnvelope<T>
where
    T: fmt::Debug + DeserializeOwned,
{
    pub meta: Meta,
    pub response: T,
}

#[derive(Debug, Deserialize)]
pub struct Meta {
    pub status: u16,
    pub msg: String,
}

impl Meta {
    pub fn is_success(&self) -> bool {
        // TODO: Swap 200 out for an http type?
        self.status == 200
    }
}

#[derive(Debug, Deserialize)]
struct Links {
    next: Link,
}

#[derive(Debug, Deserialize)]
struct Link {
    href: String,
    // TODO: Maybe use this somehow?
    method: String,
}

#[derive(Debug, Deserialize)]
struct Response {
    posts: Vec<Post>,
    #[serde(rename = "_links")]
    links: Option<Links>,
}

#[derive(Debug, Deserialize)]
struct Photo {
    url: String,
}

#[derive(Debug, Deserialize)]
struct OriginalPhoto {
    original_size: Photo,
}

#[derive(Debug, Deserialize)]
struct Post {
    id: u64,
    slug: String,
    photos: Vec<OriginalPhoto>,
}

impl Into<Vec<DownloadablePhoto>> for Post {
    fn into(self) -> Vec<DownloadablePhoto> {
        let id = self.id;
        let slug = self.slug;
        self.photos
            .into_iter()
            .enumerate()
            .map(|(index, photo)| {
                let url = photo.original_size.url;
                let ext = extension(&url);
                let filename = filename(id, &slug, index, ext);
                DownloadablePhoto { filename, url }
            })
            .collect()
    }
}

#[derive(Debug)]
struct DownloadablePhoto {
    filename: String,
    url: String,
}

fn extension(url: &str) -> &str {
    let filename = url.rsplit('/').next().expect("wtf, this has no path?");
    filename.rsplit('.').next().unwrap_or("unknown")
}

fn filename(id: u64, slug: &str, index: usize, ext: &str) -> String {
    let slug_dash = if slug == "" { "" } else { "-" };
    format!(
        "{slug}{slug_dash}{id}-{index}.{ext}",
        slug = slug,
        slug_dash = slug_dash,
        id = id,
        index = index,
        ext = ext
    )
}

async fn fetch_photo_posts(
    tx: Sender<DownloadablePhoto>,
    blog_identifier: String,
    api_key: String,
) -> anyhow::Result<()> {
    let url = format!(
        "https://api.tumblr.com/v2/blog/{}/posts/photo?api_key={}",
        blog_identifier, api_key
    );

    let mut envelope = reqwest::get(url)
        .await?
        .json::<ResponseEnvelope<Response>>()
        .await?;

    loop {
        for post in envelope.response.posts {
            let photos: Vec<DownloadablePhoto> = post.into();
            for photo in photos {
                tx.send(photo).await?;
            }
        }

        if let Some(links) = envelope.response.links {
            let url = format!(
                "https://api.tumblr.com{}&api_key={}",
                links.next.href, api_key
            );
            envelope = reqwest::get(url)
                .await?
                .json::<ResponseEnvelope<Response>>()
                .await?;
        } else {
            break;
        }
    }

    Ok(())
}

async fn fetch_and_write_photo(path: PathBuf, url: String) -> anyhow::Result<()> {
    let (file, response) = tokio::join!(tokio::fs::File::create(path), reqwest::get(url));

    let file = file?;
    let response = response?;

    let framed_write = FramedWrite::new(file, BytesCodec::new());

    let bytes = response.bytes_stream();
    bytes
        .map_err(anyhow::Error::from)
        .forward(framed_write.sink_map_err(anyhow::Error::from))
        .await?;

    Ok(())
}

async fn fetch_photos(
    mut rx: Receiver<DownloadablePhoto>,
    directory: PathBuf,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&directory).await?;
    let mut tasks = futures::stream::FuturesUnordered::new();

    loop {
        tokio::select! {
             photo_opt = rx.recv() => {
                 if let Some(photo) = photo_opt {
                     let path = directory.join(photo.filename);
                     tasks.push(fetch_and_write_photo(path, photo.url));
                 } else {
                     break;
                 }
             },
             result_opt = tasks.next() => {
                 if let Some(result) = result_opt {
                     result?;
                 }
             }
        }
    }

    tasks.try_collect().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO: Why is
    // https://thingsonhazelshead.tumblr.com/post/174913738196/we-expect-to-be-in-control-of-our-internet
    // duplicated?
    let blog_identifier = "thingsonhazelshead.tumblr.com".to_string();
    let download_directory = PathBuf::from("/tmp/pics");

    let api_key = env::var("IMAGR_TOKEN")?;

    let (tx, rx) = mpsc::channel(64);

    let join_fetch_posts = tokio::spawn(fetch_photo_posts(tx, blog_identifier, api_key.clone()));
    fetch_photos(rx, download_directory).await?;

    join_fetch_posts.await??;

    Ok(())
}
