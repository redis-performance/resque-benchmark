use crate::job::ResqueJob;
use anyhow::Result;

const BATCH_SIZE: usize = 1000;

/// Delete all benchmark queue keys and remove them from the `queues` set.
/// This is the default pre-trial cleanup — safe to use on shared Redis.
pub async fn clear_queue(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
) -> Result<()> {
    let mut pipe = redis::pipe();
    for queue in queues {
        pipe.cmd("DEL").arg(format!("queue:{queue}")).ignore();
        pipe.cmd("SREM").arg("queues").arg(queue).ignore();
    }
    pipe.query_async::<()>(conn).await?;
    Ok(())
}

/// Flush the entire database. Only called when --allow-flushdb is explicitly set.
pub async fn flushdb(conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    redis::cmd("FLUSHDB").query_async::<()>(conn).await?;
    Ok(())
}

/// Bulk-enqueue `n_jobs` Resque jobs distributed round-robin across `queues`.
///
/// Uses **RPUSH** — matching `Resque::DataStore::QueueAccess#push_to_queue`
/// (resque/lib/resque/data_store.rb:104-109), which does
/// `piped.rpush redis_key_for_queue(queue), encoded_item`. Also registers every
/// queue in the `queues` set (data_store.rb:150-152, `watch_queue`) for
/// resque-web visibility, exactly as production enqueue does.
pub async fn bulk_enqueue(
    conn: &mut redis::aio::MultiplexedConnection,
    queues: &[String],
    n_jobs: u64,
) -> Result<()> {
    // Register all queues, matching watch_queue's SADD :queues on every push
    // (data_store.rb:150-152).
    let mut sadd_pipe = redis::pipe();
    for queue in queues {
        sadd_pipe.cmd("SADD").arg("queues").arg(queue).ignore();
    }
    sadd_pipe.query_async::<()>(conn).await?;

    let n_queues = queues.len() as u64;
    let mut idx = 0u64;
    let mut remaining = n_jobs;

    while remaining > 0 {
        let batch = remaining.min(BATCH_SIZE as u64) as usize;
        let mut pipe = redis::pipe();

        for j in 0..batch {
            let queue = &queues[((idx + j as u64) % n_queues) as usize];
            let job = ResqueJob::new(idx + j as u64);
            let payload = serde_json::to_string(&job)?;
            // RPUSH, not LPUSH — see module doc above.
            pipe.rpush(format!("queue:{queue}"), payload).ignore();
        }

        pipe.query_async::<()>(conn).await?;
        idx += batch as u64;
        remaining -= batch as u64;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn queue_key_matches_redis_key_for_queue() {
        // data_store.rb:165-167: def redis_key_for_queue(queue); "queue:#{queue}"; end
        let queue = "default";
        assert_eq!(format!("queue:{queue}"), "queue:default");
    }
}
