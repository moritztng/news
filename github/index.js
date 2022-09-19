const { BigQuery } = require('@google-cloud/bigquery')
const { PubSub } = require('@google-cloud/pubsub')
const { graphql } = require('@octokit/graphql')

const graphqlWithAuth = graphql.defaults({
  headers: {
    authorization: `token ${process.env.GITHUB_TOKEN}`,
  },
})

const QUERY = `
  query ($searchQuery: String!, $cursor: String) {
    search(query: $searchQuery, type: REPOSITORY, first: 100, after: $cursor) {
      pageInfo {
        endCursor
        hasNextPage
      }
      edges {
        node {
          ... on Repository {
            owner {
              login
              ... on User {
                twitterUsername
              }
            }
            name
            stargazerCount
          }
        }
      }
    }
  }
`

async function fetchRepositories(
  graphql,
  minStars,
  maxStars,
  { repositories, cursor } = { repositories: [] }
) {
  const searchQuery = `is:public stars:${minStars}..${maxStars} sort:stars-asc`
  const result = await graphql(QUERY, { cursor, searchQuery: searchQuery })
  const resultRepositories = result.search.edges.map((edge) => edge.node)
  repositories.push(...resultRepositories)
  if (result.search.pageInfo.hasNextPage) {
    await fetchRepositories(graphql, minStars, maxStars, {
      repositories,
      cursor: result.search.pageInfo.endCursor,
    })
  }
  return repositories
}

const bigquery = new BigQuery()
const dataset = bigquery.dataset('github_repositories')
const cacheTable = dataset.table('cache')
const repositoriesTable = dataset.table('repositories')

const maxStars = parseInt(process.env.MAX_STARS)

const pubsubTopic = new PubSub({ projectId: 'news-361012' }).topic(
  'fetched-github'
)

exports.getRepositories = async (eventData, context, callback) => {
  const query = `
    SELECT stargazerCount
    FROM \`github_repositories.cache\`
    ORDER BY stargazerCount DESC
    LIMIT 1
  `
  const [job] = await bigquery.createQueryJob({
    query: query,
    location: 'US',
  })
  const [[row]] = await job.getQueryResults()
  const minStars = row ? row.stargazerCount : parseInt(process.env.MIN_STARS)

  const repositories = await fetchRepositories(
    graphqlWithAuth,
    minStars,
    maxStars
  )

  if (repositories.length > 0) {
    const time = bigquery.datetime(new Date().toISOString())
    await cacheTable.insert(
      repositories.map((repository) => ({
        time: time,
        owner: repository.owner.login,
        twitter: repository.owner.twitterUsername,
        name: repository.name,
        stargazerCount: repository.stargazerCount,
      }))
    )
  }

  if (repositories.length < 1000) {
    const query = `
      SELECT * 
      FROM (
        SELECT time, owner, twitter, name, stargazerCount, ROW_NUMBER() OVER (PARTITION BY owner, name ORDER BY time DESC) as row 
        FROM \`github_repositories.cache\`
      )
      WHERE row=1
    `
    await bigquery.createQueryJob({
      query: query,
      location: 'US',
      destination: repositoriesTable,
      writeDisposition: 'WRITE_APPEND',
    })
    const [{ schema }] = await cacheTable.getMetadata()
    await cacheTable.delete()
    dataset.createTable('cache', { schema, location: 'US' })
    await pubsubTopic.publishMessage({ data: Buffer.from('fetched github') })
    callback()
  } else {
    callback('page limit')
  }
}
