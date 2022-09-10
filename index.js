const functions = require('@google-cloud/functions-framework')
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
            name
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

functions.cloudEvent('getRepositories', async (cloudEvent) => {
  const repositories = await fetchRepositories(graphqlWithAuth)
})
