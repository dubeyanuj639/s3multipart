const express = require('express')
const app = express()
const AWS = require('aws-sdk');
var s3 = new AWS.S3({
    "accessKeyId": "***********",
    "secretAccessKey": "************",
    "region": "ap-south-1"
});
const multiparty = require('multiparty');
const fs = require('fs');

app.listen(6002, (err, done) => {
    if (err) {
        console.log("Getting Error to start the server..")
    }
    else {
        console.log("Server has started on port 6002..",)
    }
})

// Call this route to upload files in chunk....
app.get('/multipartupload/:userId', async (req, res) => {
    //The following example initiates a multipart upload.
    var form = new multiparty.Form();
    form.parse(req, async (err, fields, files) => {
        var videoUrl = await upload(files['video'][0], req.params.userId)
        console.log("This is videoUrl -->", videoUrl)
        return res.status(200).send({ "data": videoUrl })
    })
})


var fileKey;
var bucket = '********';

// Upload
var startTime = new Date();
var numPartsLeft;
var maxUploadTries;
var multipartMap = {
    Parts: []
};

function completeMultipartUpload(s3, doneParams) {
    s3.completeMultipartUpload(doneParams, function (err, data) {
        if (err) {
            console.log("An error occurred while completing the multipart upload");
            console.log(err);
        } else {
            var delta = (new Date() - startTime) / 1000;
            console.log('Completed upload in', delta, 'seconds');
            console.log('Final upload data:', data);
            console.log("this is key -->", data.Key)
        }
    });
}

function uploadPart(s3, multipart, partParams, tryNum) {
    var tryNum = tryNum || 1;
    s3.uploadPart(partParams, function (multiErr, mData) {
        if (multiErr) {
            console.log('multiErr, upload part error:', multiErr);
            if (tryNum < maxUploadTries) {
                console.log('Retrying upload of part: #', partParams.PartNumber)
                uploadPart(s3, multipart, partParams, tryNum + 1);
            } else {
                console.log('Failed uploading part: #', partParams.PartNumber)
            }
            return;
        }
        multipartMap.Parts[this.request.params.PartNumber - 1] = {
            ETag: mData.ETag,
            PartNumber: Number(this.request.params.PartNumber)
        };
        console.log("Completed part", this.request.params.PartNumber);
        console.log('mData', mData);
        if (--numPartsLeft > 0) return; // complete only when all parts uploaded

        var doneParams = {
            Bucket: bucket,
            Key: fileKey,
            MultipartUpload: multipartMap,
            UploadId: multipart.UploadId
        };

        console.log("Completing upload...");
        // return
        completeMultipartUpload(s3, doneParams);
    });
}


const upload = (file, userId) => {
    var filePath = file.path
    fileKey = userId + '/' + file.originalFilename
    var buffer = fs.readFileSync(filePath);
    // Upload
    var partNum = 0;
    var partSize = 1024 * 1024 * 5;  //Create Chunks
    numPartsLeft = Math.ceil(buffer.length / partSize);
    maxUploadTries = 3;
    var multiPartParams = { Bucket: bucket, Key: fileKey };

    return new Promise((resolve, reject) => {
        s3.createMultipartUpload(multiPartParams, function (mpErr, multipart) {
            if (mpErr) { console.log('Error!', mpErr); return; }
            console.log("Got upload ID", multipart.UploadId);
            // return
            // Grab each partSize chunk and upload it as a part
            for (var rangeStart = 0; rangeStart < buffer.length; rangeStart += partSize) {
                // console.log("@@@@-->", rangeStart, buffer.length, partSize, partNum)
                partNum++;
                var end = Math.min(rangeStart + partSize, buffer.length),
                    partParams = {
                        Body: buffer.slice(rangeStart, end),
                        Bucket: bucket,
                        Key: fileKey,
                        PartNumber: String(partNum),
                        UploadId: multipart.UploadId
                    };

                // Send a single part
                console.log('Uploading part: #', partParams.PartNumber, ', Range start:', rangeStart);
                uploadPart(s3, multipart, partParams);
            }
        });
    })
}

app.get('/getMedia/:userId', async (req, res) => {
    try {
        s3.getObject({
            Bucket: bucket,
            Key: req.query.documentName
        }).promise()
            .then(data => {
                res.writeHead(200, {
                    'Content-Type': data.ContentType,
                    'Content-Disposition': `attachment; filename=${req.query.documentName}`,
                    'Content-Length': data.ContentLength
                });
                res.end(data.Body);
            })
            .catch(err => {
                res.status(404).send({ message: 'not found' })
            });
    }
    catch (e) {
        return res.status(500).send({ "status": 500, "message": 'Internal Server Error.' });
    }
})