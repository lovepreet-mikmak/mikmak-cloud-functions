const express = require("express");
const app =  express();
app.get("/",(req, res)=>{
    const msg = req.query.message || "hello world";
        res.status(200).json({message: msg});
    })
exports.app =  app;