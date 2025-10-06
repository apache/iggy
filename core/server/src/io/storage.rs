use std::future::Future;

use compio::{
    buf::{IoBuf, IoBufMut},
    io::{AsyncReadAtExt, AsyncWriteAtExt},
};

pub trait Storage {
    fn read_exact_at<B: IoBufMut>(
        &self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = Result<B, std::io::Error>>;
    fn write_all_at<B: IoBuf>(
        &self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = Result<B, std::io::Error>>;
}

pub struct OpenOpts {
    keep_fd: bool,
    path: String,
}

impl OpenOpts {
    pub fn ephemeral(path: String) -> Self {
        Self {
            keep_fd: false,
            path,
        }
    }

    pub fn permament(path: String) -> Self {
        Self {
            keep_fd: true,
            path,
        }
    }
}

pub enum StorageImpl {
    Block(BlockStorage),
}

impl StorageImpl {
    pub fn read_exact_at<B: IoBufMut>(
        &self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = Result<B, std::io::Error>> {
        match self {
            StorageImpl::Block(storage) => storage.read_exact_at(buf, pos),
        }
    }

    pub fn write_all_at<B: IoBuf>(
        &mut self,
        buf: B,
        pos: u64,
    ) -> impl Future<Output = Result<B, std::io::Error>> {
        match self {
            StorageImpl::Block(storage) => storage.write_all_at(buf, pos),
        }
    }
}

pub struct BlockStorage {
    file: Option<compio::fs::File>,
    path: Option<String>,
}

impl BlockStorage {
    pub async fn xd(&self) {
        let file = self.file.as_ref().unwrap();
        let buf = Vec::new();
        (&*file).write_all_at(buf, 0).await.unwrap();
    }
}

impl BlockStorage {
    pub async fn new(opts: OpenOpts) -> Result<Self, std::io::Error> {
        let path = opts.path;
        let keep_fd = opts.keep_fd;
        let file = if keep_fd {
            let file = compio::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)
                .await?;
            Some(file)
        } else {
            None
        };
        let path = if file.is_some() { None } else { Some(path) };
        Ok(Self { file, path })
    }
}

impl Storage for BlockStorage {
    async fn read_exact_at<B: IoBufMut>(&self, buf: B, pos: u64) -> Result<B, std::io::Error> {
        let (result, buf) = match &self.file {
            Some(file) => file.read_exact_at(buf, pos).await.into(),
            None => {
                let path = self.path.as_ref().unwrap();
                let file = compio::fs::File::open(path).await?;
                file.read_exact_at(buf, pos).await.into()
            }
        };
        result?;
        Ok(buf)
    }

    async fn write_all_at<B: IoBuf>(&self, buf: B, pos: u64) -> Result<B, std::io::Error> {
        let (result, buf) = match self.file {
            Some(ref file) => (&*file).write_all_at(buf, pos).await.into(),
            None => {
                let path = self.path.as_ref().unwrap();
                let mut file = compio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(path)
                    .await?;
                file.write_all_at(buf, pos).await.into()
            }
        };
        result?;
        Ok(buf)
    }
}
