const router = require('express').Router();
const { Router } = require('express');
const rectifierAPI = require('../src/models/rectifier');  // Rectifier sample data
const mongoose = require('mongoose');

// 라우터 존은 간단하게 설정, :compId  --> rectId는 변수로서 다양한 값을 넣을 수 있음.
router.get('/:rectId', rectifierAPI.getRectifierByTime); 

module.exports = router;