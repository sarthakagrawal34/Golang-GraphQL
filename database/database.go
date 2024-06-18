package database

import (
	"context"
	"log"
	"time"

	"go-gql/graph/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var connectionString string = "mongodb://localhost:27017"

type DB struct {
	Client *mongo.Client
}

func Connect() *DB {
	client, err := mongo.NewClient(options.Client().ApplyURI(connectionString))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	return &DB{
		Client: client,
	}
}

func (db *DB) GetJob(id string) *model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobsCollection := db.Client.Database("graphql-job-board").Collection("jobs")
	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	var jobListing model.JobListing
	err := jobsCollection.FindOne(ctx, filter).Decode(&jobListing)
	if err != nil {
		log.Fatal(err)
	}
	return &jobListing
}

func (db *DB) GetJobs() []*model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobsCollection := db.Client.Database("graphql-job-board").Collection("jobs")
	var jobListings []*model.JobListing
	cursor, err := jobsCollection.Find(ctx, bson.D{})
	if err != nil {
		log.Fatal(err)
	}

	if err = cursor.All(ctx, &jobListings); err != nil {
		log.Fatal(err)
	}
	return jobListings
}

func (db *DB) CreateJobListing(jobInfo model.CreateJobListingInput) *model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobsCollection := db.Client.Database("graphql-job-board").Collection("jobs")
	inserted, err := jobsCollection.InsertOne(ctx, bson.M{
		"title":       jobInfo.Title,
		"description": jobInfo.Description,
		"url":         jobInfo.URL,
		"company":     jobInfo.Company,
	})
	if err != nil {
		log.Fatal(err)
	}

	var jobListing *model.JobListing
	insertedId := inserted.InsertedID.(primitive.ObjectID).Hex()
	jobListing = &model.JobListing{
		ID:          insertedId,
		Title:       jobInfo.Title,
		Description: jobInfo.Description,
		URL:         jobInfo.URL,
		Company:     jobInfo.Company,
	}
	return jobListing
}

func (db *DB) UpdateJobListing(id string, jobInfo model.UpdateJobListingInput) *model.JobListing {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jobsCollection := db.Client.Database("graphql-job-board").Collection("jobs")

	updatedInfo := bson.M{}

	if jobInfo.Title != nil {
		updatedInfo["title"] = jobInfo.Title
	}
	if jobInfo.Description != nil {
		updatedInfo["description"] = jobInfo.Description
	}
	if jobInfo.URL != nil {
		updatedInfo["url"] = jobInfo.URL
	}

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}
	update := bson.M{"$set": updatedInfo}
	results := jobsCollection.FindOneAndUpdate(ctx, filter, update, options.FindOneAndUpdate().SetReturnDocument(1))

	var jobListing model.JobListing
	if err := results.Decode(&jobListing); err != nil{
		log.Fatal(err)
	}
	
	return &jobListing
}

func (db *DB) DeleteJobListing(id string) *model.DeleteJobResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_id, _ := primitive.ObjectIDFromHex(id)
	filter := bson.M{"_id": _id}

	jobsCollection := db.Client.Database("graphql-job-board").Collection("jobs")
	_, err := jobsCollection.DeleteOne(ctx, filter)
	if err != nil{
		log.Fatal(err)
	}

	return &model.DeleteJobResponse{DeleteJobID: &id}
}
