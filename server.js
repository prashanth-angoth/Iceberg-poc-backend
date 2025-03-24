require("dotenv").config();

const peopleContrll = require("./controller.js");

const express = require("express");
const axios = require("axios");
const cors = require("cors");

const app = express();
app.use(express.json());
app.use(cors());


app.post("/api/v1/people/create", peopleContrll.createPeople);
app.post("/api/v1/people/insert",peopleContrll.insertData)
app.get("/api/v1/people/getPeoples", peopleContrll.getPeoples);
app.put("/api/v1/people/update/:id", peopleContrll.updatePeople);
app.delete("/api/v1/people/delete/:id", peopleContrll.deletePeople);
app.post("/api/v1/people/bulkDelete", peopleContrll.bulkDelete);
app.post("/api/v1/people/bulkUpdate", peopleContrll.bulkUpdate);
 


// Start server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));