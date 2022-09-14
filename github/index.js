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

functions.http('getRepositories', async (req, res) => {
  const maxStars = parseInt(process.env.MAX_STARS)
  let minStars = parseInt(process.env.MIN_STARS)
  let repositories = []
  let repositoriesSlice = []
  do {
    repositoriesSlice = await fetchRepositories(
      graphqlWithAuth,
      minStars,
      maxStars
    )
    repositories = repositories.concat(repositoriesSlice)
    minStars = repositories[repositories.length - 1].stargazerCount
  } while (repositoriesSlice.length >= 1000)
  
  repositories = repositories.filter(
    (value, index, self) =>
      index ===
      self.findIndex(
        (t) => t.owner.login === value.owner.login && t.name === value.name
      )
  )
  
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
})
