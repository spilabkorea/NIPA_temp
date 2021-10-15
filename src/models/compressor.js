// compressor 데이터에 대한 정보 및 Shcema 설정
const mongoose = require('mongoose');
mongoose.set('debug', true)

// Schema 정보 입력
const compSchema = new mongoose.Schema({
    _id :{type : Object},
    compressorId : {type : String},
    time : {type : Number},
    anomaly_score : {type : Number},
    anomaly : {type : Number}
    }
);
// compressorId 정보 담기
const compIdList = ['364','365','366']

let compressor = {}

// compressorId 마다 collection에 연결하는 객체 생성 및, 객체 비구조화 진행
compIdList.forEach((id)=>{
  let model = mongoose.model(`compressor${id}`, compSchema, `compressor${id}`);
  compressor[`compressor${id}`] = model;
})

// compressorId를 입력하면, 위에서 만든 모델을 반환하는 함수 생성
function getCompressorModel(compId){
    return compressor[`compressor${compId}`]
}

// startTime(require), endTime(requre)으로 데이터 조회할 수 있도록 파라미터 추가.
exports.getCompressorByTime = (req,res) =>{
    const {compId} = req.params;
    const startTime = req.query.startTime;
    const endTime = req.query.endTime;

    // 파라미터에 맞는 데이터 조회
    var Compressor = getCompressorModel(compId)
    Compressor
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

//exports.getCompressorByAnomaly = (req, res) => {
//    const {compId} = req.params;
//    const {anomaly} = req.params;
//   var Compressor = getCompressorModel(compId)
    
//    Compressor
//    .find()
//    .where('anomaly')
//    .equals(anomaly)
//    .then((datas)=>{
//        console.log(datas.length)
//        if (!datas.length){
//            return res.status(404).send({ err: 'Data not found' });
//        }else {
//            return res.status(200).json(datas);
//        }
//    })
//    .catch(e => res.status(500).send(e));
//}