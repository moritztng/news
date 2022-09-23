import fetch from 'node-fetch'

export async function getTweets(eventData, context, callback) {
  const startTime = new Date()
  const endTime = new Date()

  startTime.setHours(endTime.getHours() - 24)
  endTime.setMinutes(startTime.getMinutes() - 10)

  const body = {
    recentSearch: {
      query: process.env.QUERY,
      maxResults: 100,
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      category: process.env.CATEGORY,
      subCategory: process.env.SUB_CATEGORY,
    },
    dataSet: {
      newDataSet: process.env.NEW_DATASET === 'true',
      dataSetName: process.env.DATASET_NAME,
    },
  }

  await fetch('https://news-361012.uc.r.appspot.com/search', {
    method: 'post',
    body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json' },
  })

  callback()
}
