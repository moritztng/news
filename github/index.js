const functions = require('@google-cloud/functions-framework')
const { BigQuery } = require('@google-cloud/bigquery')
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

exports.getRepositories = async (eventData, context, callback) => {
  const query = `
    SELECT stargazerCount
    FROM \`github_repositories.repositories\`
    WHERE time > '${new Date().toISOString().split('T')[0]}'
    ORDER BY stargazerCount DESC
    LIMIT 1
  `

  const [job] = await bigquery.createQueryJob({ query: query, location: 'US' })
  const [[row]] = await job.getQueryResults()
  const maxStars = parseInt(process.env.MAX_STARS)
  let minStars = row ? row.stargazerCount : parseInt(process.env.MIN_STARS)
  const repositories = await fetchRepositories(
    graphqlWithAuth,
    minStars,
    maxStars
  )
  
  if (repositories.length > 0) {
    const time = bigquery.datetime(new Date().toISOString())
    await bigquery
      .dataset('github_repositories')
      .table('repositories')
      .insert(
        repositories.map((repository) => ({
          time: time,
          owner: repository.owner.login,
          twitter: repository.owner.twitterUsername,
          name: repository.name,
          stargazerCount: repository.stargazerCount,
        }))
      )
  }
  
  callback(repositories.length < 1000 ? null : 'page limit')
}
