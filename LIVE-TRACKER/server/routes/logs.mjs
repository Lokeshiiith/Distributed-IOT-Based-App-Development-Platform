import express from "express";
import db from "../db/conn.mjs";
import { ObjectId } from "mongodb";

const router = express.Router();
// Get a list of 50 posts
router.get("/", async (req, res) => {
  try {
    const { collection = '', limit = 10 } = req.query;
    const newLimit = parseInt(limit, 10);
    let collectionName = await db.collection(collection);
    let results = await collectionName.find({})
      .limit(newLimit)
      .toArray();
    res.send(results).status(200);
  } catch (e) {
    console.log(e);
  }
});
//standardMonitoring
//alertedMonitoring
// Fetches the latest posts

router.get("/health", async (req, res) => {
  try {
    let collectionName = await db.collection('standardMonitoring');
    const standardMonitoringResults = await collectionName.find({}).toArray();
    collectionName = await db.collection('alertedMonitoring');
    const alertedMonitoringResults = await collectionName.find({}).toArray();
    res.send({available: standardMonitoringResults, alert: alertedMonitoringResults});
  } catch (e) {
    console.log(e);
  }
})


router.get("/latest", async (req, res) => {
  let collection = await db.collection("posts");
  let results = await collection.aggregate([
    { "$project": { "author": 1, "title": 1, "tags": 1, "date": 1 } },
    { "$sort": { "date": -1 } },
    { "$limit": 3 }
  ]).toArray();
  res.send(results).status(200);
});

// Get a single post
// router.get("/:id", async (req, res) => {
//   let collection = await db.collection("posts");
//   let query = { _id: ObjectId(req.params.id) };
//   let result = await collection.findOhttps://meet.google.com/ixx-jxxs-hamne(query);

//   if (!result) res.send("Not found").status(404);
//   else res.send(result).status(200);
// });

// Add a new document to the collection
router.post("/", async (req, res) => {
  let collection = await db.collection("posts");
  let newDocument = req.body;
  newDocument.date = new Date();
  let result = await collection.insertOne(newDocument);
  res.send(result).status(204);
});

// Update the post with a new comment
router.patch("/comment/:id", async (req, res) => {
  const query = { _id: ObjectId(req.params.id) };
  const updates = {
    $push: { comments: req.body }
  };

  let collection = await db.collection("posts");
  let result = await collection.updateOne(query, updates);

  res.send(result).status(200);
});

// Delete an entry
router.delete("/:id", async (req, res) => {
  const query = { _id: ObjectId(req.params.id) };

  const collection = db.collection("posts");
  let result = await collection.deleteOne(query);

  res.send(result).status(200);
});

export default router;
