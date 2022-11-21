const fs = require("fs");
const { parse } = require("csv-parse");
const { BlobServiceClient } = require("@azure/storage-blob");
require("dotenv").config();
const { v1: uuidv1 } = require("uuid");
const AbortControllerAZ = require("@azure/abort-controller").AbortController;


const rowsChunks = [[]];
const CHUNK_LIMIT = 20;

const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
if (!AZURE_STORAGE_CONNECTION_STRING) throw Error('Azure Storage Connection string not found');
const blobServiceClient = BlobServiceClient.fromConnectionString(
    AZURE_STORAGE_CONNECTION_STRING
);
const containerClient = blobServiceClient.getContainerClient('picture-books-final');
 
const uploadChunk = async (chunck, i) => {
    const reqFactory = (url) => fetchWithTimeout(url.replace('http://', 'https://')).then(r => r.blob().then(b => b.arrayBuffer()));

    await Promise.all(
        chunck.map(async (row) => {
            const getImgP = row[5].length && row[5].includes('http') ? reqFactory(row[5]) : null;
            const getImgM = row[6].length && row[6].includes('http') ? reqFactory(row[6]) : null;
            const getImgL = row[7].length && row[7].includes('http') ? reqFactory(row[7]) : null;
            const imagesBuffers = await Promise.allSettled([getImgP, getImgM, getImgL]);
            const uploadedImages = [null, null, null];
            await Promise.allSettled(
                imagesBuffers.map(async ({value: buffer}, i) => {
                    const blobName = uuidv1() + '.jpeg';
                    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
                    if(buffer) {
                        try {
                            await blockBlobClient.uploadData(buffer, {abortSignal: AbortControllerAZ.timeout(3000)});
                            uploadedImages[i] = blockBlobClient.url;
                        } catch (err) {
                            console.log(err);
                        }

                    }
                })
            )

            outputCsv.write([...row, ...uploadedImages].join(';') + '\n', ()=>{});
        })
    );
    console.log(`chunk ${i+1} sended`);
}

async function fetchWithTimeout(resource, options = {}) {
    const { timeout = 3000 } = options;
    
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    const response = await fetch(resource, {
      ...options,
      signal: controller.signal  
    });
    clearTimeout(id);
    return response;
  }

const outputCsv = fs.createWriteStream('./output.csv');
outputCsv.write(["ISBN", "Book-Title", "Book-Author", "Year-Of-Publication","Publisher","Image-URL-S",
    "Image-URL-M", "Image-URL-L", "Image-URL-S-Azure", "Image-URL-M-Azure", "Image-URL-L-Azure"
].join(';') + '\n', () => {});

fs.createReadStream('./BX-Books.csv')
    .pipe(parse({delimiter: ";", from_line: 2}))
    .on("data", (row) => {
        if(rowsChunks[rowsChunks.length-1].length < CHUNK_LIMIT) {
            rowsChunks[rowsChunks.length-1].push(row);
        } else {
            rowsChunks[rowsChunks.length] = [row];
        }
        
    })
    .on("end", async () => {
        console.log('chunks: ', rowsChunks.length);
        for (const [i, chunck] of rowsChunks.slice(6).entries()) {
            await uploadChunk(chunck, i);
        }
       
        outputCsv.end();
        console.log('Finalizado');
    });