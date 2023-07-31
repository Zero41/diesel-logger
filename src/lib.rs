extern crate diesel;
#[macro_use]
extern crate log;

use std::fmt::Display;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use diesel::backend::Backend;
use diesel::debug_query;
use diesel::prelude::*;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel_async::{
    AsyncConnection, SimpleAsyncConnection, TransactionManager, TransactionManagerStatus,
};
use futures_util::future::BoxFuture;
use futures_util::FutureExt;

/// Wraps a diesel `Connection` to time and log each query using
/// the configured logger for the `log` crate.
///
/// Currently, this produces a `debug` log on every query,
/// an `info` on queries that take longer than 1 second,
/// and a `warn`ing on queries that take longer than 5 seconds.
/// These thresholds will be configurable in a future version.
pub struct LoggingConnection<C>
where
    C: AsyncConnection,
{
    connection: C,
    transaction_manager: LoggingTransactionManager<C>,
}

#[async_trait::async_trait]
impl<C> SimpleAsyncConnection for LoggingConnection<C>
where
    C: AsyncConnection,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.connection.batch_execute(query).await
    }
}

#[async_trait::async_trait]
impl<C> AsyncConnection for LoggingConnection<C>
where
    C: AsyncConnection + 'static,
    <C as AsyncConnection>::Backend: std::default::Default,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    type LoadFuture<'conn, 'query> = <C as AsyncConnection>::LoadFuture<'conn, 'query>;
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type Stream<'conn, 'query> = <C as AsyncConnection>::Stream<'conn, 'query>;
    type Row<'conn, 'query> = <C as AsyncConnection>::Row<'conn, 'query>;
    type Backend = <C as AsyncConnection>::Backend;
    type TransactionManager = LoggingTransactionManager<C>;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        Ok(LoggingConnection::new(C::establish(database_url).await?))
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + Send + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + Send + 'query,
    {
        let query = source.as_query();
        let debug_query = debug_query::<Self::Backend, _>(&query);
        let debug_string = format!("{}", debug_query);

        let begin = Self::bench_query_begin();
        let res = self.connection.load(query);
        Self::bench_query_end(begin, &debug_string);
        res
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + Send + 'query,
    {
        let debug_query = debug_query::<Self::Backend, _>(&source);
        let query_sql = format!("{}", debug_query);

        async move {
            let start_time = Instant::now();
            let result = self.connection.execute_returning_count(source).await;
            let duration = start_time.elapsed();
            log_query(&query_sql, duration);
            result
        }.boxed()
    }

    fn transaction_state(&mut self) -> &mut LoggingTransactionManager<C> {
        &mut self.transaction_manager
    }
}

impl<C> LoggingConnection<C>
where
    C: AsyncConnection,
{
    // fn bench_query<'conn, 'query, T>(
    //     query: T,
    //     func: <LoggingConnection as AsyncConnection>::ExecuteFuture<'conn, 'query>,
    // ) -> <LoggingConnection as AsyncConnection>::ExecuteFuture<'conn, 'query>
    // where
    //     T: QueryFragment<<LoggingConnection as AsyncConnection>::Backend> + QueryId + Send + 'query,
    // {
    //     let debug_query = debug_query::<<LoggingConnection as AsyncConnection>::Backend, _>(&query);
    //     // Self::bench_query_str(&debug_query, func).await
    //     todo!()
    // }

    // async fn bench_query_str<F, Fut, R>(query: &dyn Display, mut func: F) -> R
    // where
    //     F: FnMut() -> Fut,
    //     Fut: Future<Output = R>,
    // {
    //     let start_time = Instant::now();
    //     let result = func().await;
    //     let duration = start_time.elapsed();
    //     log_query(&query, duration);
    //     result
    // }

    fn bench_query_begin() -> Instant {
        Instant::now()
    }

    fn bench_query_end(start_time: Instant, query: &dyn Display) {
        let duration = start_time.elapsed();
        log_query(&query, duration);
    }
}

impl<C> LoggingConnection<C>
where
    C: AsyncConnection,
{
    pub fn new(connection: C) -> Self {
        Self {
            connection,
            transaction_manager: LoggingTransactionManager::<C> {
                phantom: PhantomData,
            },
        }
    }
}

#[derive(Default)]
pub struct LoggingTransactionManager<C>
where
    C: AsyncConnection,
{
    phantom: PhantomData<C>,
}

#[async_trait::async_trait]
impl<C> TransactionManager<LoggingConnection<C>> for LoggingTransactionManager<C>
where
    C: AsyncConnection + 'static,
    <C as AsyncConnection>::Backend: std::default::Default,
    <C::Backend as Backend>::QueryBuilder: Default,
{
    type TransactionStateData = Self;

    async fn begin_transaction(conn: &mut LoggingConnection<C>) -> QueryResult<()> {
        <<C as AsyncConnection>::TransactionManager as TransactionManager<C>>::begin_transaction(
            &mut conn.connection,
        )
        .await
    }

    async fn rollback_transaction(conn: &mut LoggingConnection<C>) -> QueryResult<()> {
        <<C as AsyncConnection>::TransactionManager as TransactionManager<C>>::rollback_transaction(
            &mut conn.connection,
        )
        .await
    }

    async fn commit_transaction(conn: &mut LoggingConnection<C>) -> QueryResult<()> {
        <<C as AsyncConnection>::TransactionManager as TransactionManager<C>>::commit_transaction(
            &mut conn.connection,
        )
        .await
    }

    fn transaction_manager_status_mut(
        conn: &mut LoggingConnection<C>,
    ) -> &mut TransactionManagerStatus {
        <<C as AsyncConnection>::TransactionManager as TransactionManager<
            C,
        >>::transaction_manager_status_mut(&mut conn.connection)
    }
}

fn log_query(query: &dyn Display, duration: Duration) {
    if duration.as_secs() >= 5 {
        warn!(
            "SLOW QUERY [{:.2} s]: {}",
            duration_to_secs(duration),
            query
        );
    } else if duration.as_secs() >= 1 {
        info!(
            "SLOW QUERY [{:.2} s]: {}",
            duration_to_secs(duration),
            query
        );
    } else {
        debug!("QUERY: [{:.1}ms]: {}", duration_to_ms(duration), query);
    }
}

const NANOS_PER_MILLI: u32 = 1_000_000;
const MILLIS_PER_SEC: u32 = 1_000;

fn duration_to_secs(duration: Duration) -> f32 {
    duration_to_ms(duration) / MILLIS_PER_SEC as f32
}

fn duration_to_ms(duration: Duration) -> f32 {
    (duration.as_secs() as u32 * 1000) as f32
        + (duration.subsec_nanos() as f32 / NANOS_PER_MILLI as f32)
}
