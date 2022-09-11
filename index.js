const functions = require('@google-cloud/functions-framework')
const { BigQuery} = require('@google-cloud/bigquery')
const { graphql } = require('@octokit/graphql')

const graphqlWithAuth = graphql.defaults({
  headers: {
    authorization: `token ${process.env.GITHUB_TOKEN}`,
  },
})

const QUERY = `
  query ($cursor: String) {
    search(query: "is:public stars:900..1100", type: REPOSITORY, first: 100, after: $cursor) {
      pageInfo {
        endCursor
        hasNextPage
      }
      edges {
        node {
          ... on Repository {
            owner {
              login
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
  { repositories, cursor } = { repositories: [] }
) {
  const result = await graphql(QUERY, { cursor })
  const resultRepositories = result.search.edges.map((edge) => edge.node)

  repositories.push(...resultRepositories)

  if (result.search.pageInfo.hasNextPage) {
    await fetchRepositories(graphql, {
      repositories,
      cursor: result.search.pageInfo.endCursor,
    })
  }

  return repositories
}

const bigquery = new BigQuery()

functions.http('getRepositories', async (req, res) => {
  const repositories = await fetchRepositories(graphqlWithAuth)
  const time = bigquery.datetime(new Date().toISOString())
  await bigquery.dataset('github_repositories').table('repositories').insert(repositories.map((repository) => ({time: time, owner: repository.owner.login, name: repository.name, stargazerCount: repository.stargazerCount})))
  res.send('OK')
})
