// import express as express 와 같은 내용
const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const mongoose = require('mongoose')
/*
  body-parser
  : 요청의 본문을 해석해주는 미들웨어이다.
    보통 폼 데이터나 AJAX요청의 데이터를 처리한다
*/
app.use(bodyParser.urlencoded({extended:true}));
app.use(bodyParser.json());

/*
  그런데 이전에 express미들웨어에서의 app.js에서는 body-parser를 사용하지 않았다.
  익스프레스 4.16.0버전 부터 body-parser의 일부 기능이 익스프레스에 내장되었기 때문이다.
*/
app.use(express.urlencoded({ extended: true }))   ;
app.use(express.json());

/*
  Raw는 본문이 버퍼 데이터일 때, Text는 본문이 텍스트 데이터일 때  해석하는 미들웨어이다.
  서비스에 적용하고 싶다면 body-parser를 설치한 후 다음과 같이 추가한다.
*/
app.use(bodyParser.text());

// env 환경변수 안에 MONGO_URI, PORT 넣음
const { MONGO_URI, PORT } = process.env;  
// 환경변수에 MONGO_URI, PORT가 있음을 명시
if (!MONGO_URI) console.error("MONGO_URI is required!!!");
if (!PORT) console.error("MONGO_URI is required!!!");



// 비동기 처리 : 몽고디비 연결 후 서버 접속 --> 보통 서버가 먼저 연결 되는데
// 서버 연결 후 mongodb 연결 되기 전에 GET 요청이 오면 오류 발생
// async(), await를 사용해서 순서대로 처리.
const server = async() => {
  // try, catch : 예외처리 함수
  try {
    // env 환경변수 안에 MONGO_URI, PORT 넣음(객체 비 구조화)
    const { MONGO_URI, PORT } = process.env;  
    // 환경변수에 MONGO_URI, PORT가 있음을 명시
    if (!MONGO_URI) throw new Error("MONGO_URI is required!!!");
    if (!PORT) throw new Error("PORT is required!!!");
    
    // CONNECT to MongoDB 그리고 Mongodb 연결 정보 출력
    let mongodbConnection = await mongoose.connect(MONGO_URI,{useNewUrlParser: true, useUnifiedTopology: true})
    console.log({mongodbConnection})    

    // ROUTERS 생성, compressor와 rectifier로 분리
    app.use('/data/ai/rect', require('../routes/rect_api'));
    app.use('/data/ai/comp', require('../routes/comp_api'));

    // Web server listening on port
    app.listen(PORT, () => console.log(`Server lisetening on port ${PORT}`));   
    
    // try, catch 문법에서 catch는 오류가 발생했을 때의 상황
  } catch(err) {
    console.log(err);
  }
}

// server라는 function을 만들어 줬으므로 실행시켜줘야 함.
server();

