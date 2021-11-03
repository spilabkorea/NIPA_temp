// compressor 데이터에 대한 정보 및 Shcema 설정/
const mongoose = require('mongoose');
mongoose.set('debug', true)

// Schema 정보 입력
const rectSchema = new mongoose.Schema({
    _id : {type : Object},
    rectifierId : {type : String},
    time : {type : Number},
    anomaly_score : {type : Number},
    anomaly : {type : Number}
  }
);
  





// rectifierId 정보 담기
const rectIdList = ['364', '365', '366']

let rectifier = {}

// rectifierId 마다 collection에 연결하는 객체 생성
rectIdList.forEach((id)=>{
  let model = mongoose.model(`rectifier${id}`, rectSchema, `rectifier${id}`)
  rectifier[`rectifier${id}`] = model;
})

// rectifierId를 입력하면, 위에서 만든 모델을 반환하는 함수 생성(객체 비구조화 진행)
function getRectifierModel(rectId){
  return rectifier[`rectifier${rectId}`]
}

// startTime(require), endTime(requre)으로 데이터 조회할 수 있도록 파라미터 추가.
exports.getRectifierByTime = (req,res) =>{
  const {rectId} = req.params;
  const startTime = req.query.startTime;
  const endTime = req.query.endTime;

  // 파라미터에 맞는 데이터 조회
  var Rectifier = getRectifierModel(rectId)
  Rectifier
      .find()
      .where('time')
      .gte(startTime)
      .lte(endTime)
      .then((datas)=>{
          console.log(datas.length)
          if (!datas.length){
              return res.status(404).send({ err: 'Data not found' });
          }else {
              return res.status(200).json(datas);
          }
      })
      .catch(e => res.status(500).send(e));
}
