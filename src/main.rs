use antidotec::connection::*;
use fuse::Filesystem;

const MOUNT: &str = "/mnt/rebeccapurple";

struct AntidoteFs;

fn main() -> Result<(), antidotec::connection::Error> {
    async_std::task::block_on(async {
        let fuse_task = async_std::task::spawn_blocking(|| {
            let options = ["-o", "ro", "-o", "fsname=hello"]
                .iter()
                .map(|o| o.as_ref())
                .collect::<Vec<&std::ffi::OsStr>>();

            fuse::mount(AntidoteFs, &std::path::Path::new(MOUNT), &options).unwrap();
        });

        let mut connection = Connection::new("127.0.0.1:8101").await?;
        let mut tx = connection.start_transaction().await?;
        {
            let bucket_id = vec![0];
            let counter_id = vec![0];

            tx.update(bucket_id.clone(), vec![
                counter::inc(counter_id.clone(), 5),
            ]).await?;

            let reads = tx.read(bucket_id, vec![
                counter::get(counter_id.clone()),
            ]).await?;

            println!("{:?}", reads.counter(0));
        }
        tx.commit().await?;
        Ok(())
    });

}
