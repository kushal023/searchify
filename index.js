const express = require('express');
require('dotenv/config');
const Port = process.env.PORT;
const axios = require('axios');
const app = express();
const esUrl = process.env.ES_URL;
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.post('/create-index', async (req, res) => {
  try {
    const checkIndexExist = () => {
      new Promise((resolve) => {
        axios
          .get(`${esUrl}${req.body.index}`)
          .then((_) => {
            resolve(true);
          })
          .catch(() => {
            resolve(false);
          });
      });
    };
    const ifIndexExist = await checkIndexExist();
    if (!ifIndexExist) {
      console.log(
        '`${esUrl}${req.body.index}`',
        `${esUrl}${req.body.index}`
      );
      const esResponse = await axios.put(
        `${esUrl}${req.body.index}`,
        {
          mappings: {
            properties: {
              name: {
                type: 'text',
              },
              email: {
                type: 'text',
                fields: {
                  raw: {
                    type: 'keyword',
                  },
                },
              },
              country: {
                type: 'text',
              },
              age: {
                type: 'integer',
              },
              company: {
                type: 'text',
              },
              jobRole: {
                type: 'text',
              },
              description: {
                type: 'text',
              },
              createdAt: {
                type: 'date',
                fields: {
                  raw: {
                    type: 'keyword',
                  },
                },
              },
            },
          },
        }
      );
      console.log('*********');
      // console.log('::::esResponse', esResponse);
      res.json(esResponse.data);
    } else {
      res.json('Index exists already');
    }
  } catch (error) {
    res.status(500).json(error);
  }
});
app.post('/data', async (req, res) => {
  try {
    const sampleData = require('./sample.json');
    for (let row of Object.keys(sampleData)) {
      await axios.post(
        `${esUrl}${req.body.index}/_doc`,
        sampleData[row]
      );
    }
    res.json('Bulk data inserted');
  } catch (error) {
    res.status(500).json(error);
  }
});

app.get('/data/:index', async (req, res) => {
  try {
    let response;
    const test = req.query.test;

    switch (test) {
      case 'sorting':
        response = await axios.post(
          `${esUrl}${req.params.index}/_search`,
          {
            sort: {
              createdAt: 'desc',
            },
          }
        );
        break;

      case 'matching':
        response = await axios.post(
          `${esUrl}${req.params.index}/_search`,
          {
            query: {
              match: {
                country: 'samoa',
              },
            },
          }
        );
        break;

      case 'multi-matching':
        response = await axios.post(
          `${esUrl}${req.params.index}/_search`,
          {
            query: {
              bool: {
                must: [
                  {
                    match: {
                      name: 'Anastacio Stamm',
                    },
                  },
                  {
                    match: {
                      country: 'Samoa',
                    },
                  },
                ],
              },
            },
          }
        );
        break;

      default:
        response = await axios.get(
          `${esUrl}${req.params.index}/_search`
        );
        break;
    }

    res.json(response.data);
  } catch (error) {
    res.json(error);
  }
});

app.delete('/data/index/:id', async (req, res) => {
  try {
    const response = await axios.delete(
      `${esUrl}${req.params.index}/_doc/${req.params.id}`
    );
    res.json(response.data);
  } catch (error) {
    res.status(400).json(error);
  }
  res.status(200).json(`Deleted data ${req.params.id}`);
});

app.listen(Port, () => {
  console.log(`localhost:${Port}`);
});
