const router = require('express').Router();
const { Router } = require('express');
const compSchema = require('../src/models/compressor');  // Rectifier sample data

//var compressor364 = Base.compressor364; // compressor364 Result data
//var compressor365 = Base.compressor365; // compressor365 Result data
//var compressor366 = Base.compressor366; // compressor366 Result data

const mongoose = require('mongoose');

const compressor364 = mongoose.model('compressor364', compSchema, 'compressor364');
const compressor365 = mongoose.model('compressor365', compSchema, 'compressor365');
const compressor366 = mongoose.model('compressor366', compSchema, 'compressor366');

const compTest = [compressor364,compressor365,compressor366];

function getCompressorModel(compId){
  return compTest[+compId-364]
}
/*
  GET: get All Data
*/

router.get('/364', function(req, res){ // 콜백 함수 정의
    compressor364.find()
    .then((datas) => {
      console.log(datas.length);
      if (!datas.length) return res.status(404).send({ err: 'Data not found' });
      res.status(200).json(datas);
    })
    .catch(e => res.status(500).send(e));
});

router.get('/365', function(req, res){ // 콜백 함수 정의
    compressor365.find()
    .then((datas) => {
      console.log(datas.length);
      if (!datas.length) return res.status(404).send({ err: 'Data not found' });
      res.status(200).json(datas);
    })
    .catch(e => res.status(500).send(e));
});

router.get('/366', function(req, res){ // 콜백 함수 정의
    compressor366.find()
    .then((datas) => {
      console.log(datas.length);
      if (!datas.length) return res.status(404).send({ err: 'Data not found' });
      res.status(200).json(datas);
    })
    .catch(e => res.status(500).send(e));
});




// 추가 버젼

router.get('/:compId', function(req, res){ // 콜백 함수 정의

  const {compId} = req.params;
  const compressor = getCompressorModel(compId)
  compressor.find()
  .then((datas) => {
    console.log(datas.length);
    if (!datas.length) return res.status(404).send({ err: 'Data not found' });
    res.status(200).json(datas);
  })
  .catch(e => res.status(500).send(e));
});




module.exports = router;