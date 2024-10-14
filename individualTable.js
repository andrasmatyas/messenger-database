import { readdirSync, readFileSync } from 'node:fs'
import path from 'node:path'
import pg from 'pg'
import 'dotenv/config'
const { Pool } = pg
const pool = new Pool({
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
})
const fullDate = (ts = 0) => {
  const a = new Date(ts)
  const months = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec',
  ]
  return `${a.getFullYear()}-${months[a.getMonth()]}-${a
    .getDate()
    .toString()
    .padStart(2, '0')} ${a.getHours()}:${a
    .getMinutes()
    .toString()
    .padStart(2, '0')}:${a.getSeconds().toString().padStart(2, '0')}`
}
//Set folder path --------------------------------------------------------------------------------------------------------
const folderPath = process.env.INDIVIDUAL_FOLDER
//------------------------------------------------------------------------------------------------------------------------
//Get number of message files
const nrMessage = readdirSync(folderPath).filter((fn) =>
  fn.includes('message_')
).length
//Generate table name
const folderName = path.basename(folderPath)
const fileSplit = folderName.includes('_')
  ? folderName.split('_')
  : ['t', folderName]
if (fileSplit[0] === 'facebookuser') {
  fileSplit[0] = 't'
}
const tableName =
  fileSplit[0] === 't'
    ? fileSplit[0].concat('_', fileSplit[1])
    : fileSplit[0].concat('_', fileSplit[1].substring(0, 1))
const tableName62 =
  tableName.length > 62 ? tableName.substring(0, 62) : tableName
//Drop table
try {
  await pool.query(`DROP TABLE ${tableName62}`)
} catch {}
//Create table
const createQuery = `CREATE TABLE ${tableName62} (sender_name varchar(255),timestamp_ms bigint,timestamp_db timestamp,content text,reactions varchar(10),reactions_actor varchar(255),call_duration integer,is_unsent boolean,audio_files boolean,files boolean,gifs boolean,photos boolean,videos boolean,sticker boolean,share boolean,uri_link text)`
try {
  const res = await pool.query(createQuery)
  console.log(`${res.command} executed successfully!`)
} catch (err) {
  console.log(err.message)
}
for (let i = 1; i <= nrMessage; i++) {
  //Read JSON
  const messageData = JSON.parse(
    readFileSync(folderPath.concat('\\', `message_${i}.json`)),
    (key, value) => {
      if (typeof value === 'string') {
        let buff = Buffer.from(value, 'latin1')
        return buff.toString('utf-8')
      } else {
        return value
      }
    }
  )
  //Process messages
  for (let m of messageData.messages) {
    //Fill values
    let sender_name = m.sender_name ? m.sender_name : ''
    let timestamp_ms = m.timestamp_ms ? m.timestamp_ms : 0
    let timestamp_db = fullDate(timestamp_ms)
    let content = m.content ? m.content : ''
    let reactions = m.reactions ? m.reactions[0].reaction : ''
    let reactions_actor = m.reactions ? m.reactions[0].actor : ''
    let call_duration = m.call_duration ? m.call_duration : 0
    let is_unsent = m.is_unsent ? true : false
    let audio_files = m.audio_files ? true : false
    let files = m.files ? true : false
    let gifs = m.gifs ? true : false
    let photos = m.photos ? true : false
    let videos = m.videos ? true : false
    let sticker = m.sticker ? true : false
    let share = m.share?.link ? true : false
    let uri_link = ''
    if (audio_files) uri_link = m.audio_files[0].uri
    if (files) uri_link = m.files[0].uri
    if (gifs) uri_link = m.gifs[0].uri
    if (photos) uri_link = m.photos[0].uri
    if (videos) uri_link = m.videos[0].uri
    if (sticker) uri_link = m.sticker.uri
    if (share) uri_link = m.share.link
    if (is_unsent) content = '>>This message has been deleted<<'
    //Run INSERT query
    const insertQuery = `INSERT INTO ${tableName62} (sender_name,timestamp_ms,timestamp_db,"content",reactions,reactions_actor,call_duration,is_unsent,audio_files,files,gifs,photos,videos,sticker,"share",uri_link) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`
    try {
      await pool.query(insertQuery, [
        sender_name,
        timestamp_ms,
        timestamp_db,
        content,
        reactions,
        reactions_actor,
        call_duration,
        is_unsent,
        audio_files,
        files,
        gifs,
        photos,
        videos,
        sticker,
        share,
        uri_link,
      ])
    } catch (err) {
      console.log(err.message)
    }
  }
  console.log(`message_${i}.json loaded successfully!`)
}
