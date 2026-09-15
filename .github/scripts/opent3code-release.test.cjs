const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const crypto = require("node:crypto");
const { validateManifest, assertRun, verifyPayload } = require("./opent3code-release.cjs");
const sha = "a".repeat(40);
const run = { repository: {id:1341460159}, head_repository:{id:1341460159}, head_branch:"main", head_sha:sha, path:".github/workflows/ci.yml", event:"push", status:"completed", conclusion:"success" };
const jobs = ["PR Gate","OpenT3Code Maintenance","Check","Test","Test Server 1","Test Server 2","Test Server 3","Rust","Release Smoke"].map(name=>({name,status:"completed",conclusion:"success"}));
test("source manifest cannot select a stable release or upstream tag",()=>{
 assert.equal(validateManifest({enabled:true,sourceOnly:true,version:"0.1.0-alpha.1"}).tag,"opent3code-v0.1.0-alpha.1");
 for(const patch of [{enabled:false},{sourceOnly:false},{version:"1.0.0"},{version:"../bad"},{version:"0.1.0-alpha.0"}]) assert.throws(()=>validateManifest({enabled:true,sourceOnly:true,version:"0.1.0-alpha.1",...patch}));
});
test("release requires the exact main push and every applicable CI job",()=>{
 assert.doesNotThrow(()=>assertRun(run,sha,jobs));
 for(const patch of [{head_sha:"b".repeat(40)},{event:"pull_request"},{path:"other.yml"},{head_branch:"feature"},{conclusion:"failure"},{status:"in_progress"},{repository:{id:1}},{head_repository:{id:1}}]) assert.throws(()=>assertRun({...run,...patch},sha,jobs));
 for(let index=0;index<jobs.length;index++) {
   for(const conclusion of ["failure","cancelled","skipped",null]) assert.throws(()=>assertRun(run,sha,jobs.map((job,i)=>i===index?{...job,conclusion}:job)));
   assert.throws(()=>assertRun(run,sha,jobs.filter((_,i)=>i!==index)));
 }
 assert.throws(()=>assertRun(run,sha,[...jobs,jobs[0]]));
});
test("asset receipt, checksums, extra files and path traversal fail closed",()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),"opent3-release-"));
 const version="0.1.0-alpha.1";
 try {
   const names=[`opent3code-source-${version}.zip`,`opent3code-dsh-plugin-${version}.tgz`,"RELEASE.json"];
   fs.writeFileSync(path.join(root,names[0]),"source fixture");fs.writeFileSync(path.join(root,names[1]),"plugin fixture");
   fs.writeFileSync(path.join(root,"RELEASE.json"),JSON.stringify({sha,version,ciRun:1,sourceOnly:true}));
   const sums=names.map(name=>crypto.createHash("sha256").update(fs.readFileSync(path.join(root,name))).digest("hex")+"  "+name).join("\n")+"\n";
   fs.writeFileSync(path.join(root,"SHA256SUMS"),sums);
   assert.equal(verifyPayload(root,version,sha,1).length,4);
   assert.throws(()=>verifyPayload(root,version,"b".repeat(40),1));
   assert.throws(()=>verifyPayload(root,version,sha,2));
   fs.writeFileSync(path.join(root,"extra"),"unexpected");assert.throws(()=>verifyPayload(root,version,sha,1));fs.unlinkSync(path.join(root,"extra"));
   fs.writeFileSync(path.join(root,"SHA256SUMS"),sums.replace(names[0],"../secret"));assert.throws(()=>verifyPayload(root,version,sha,1));
   fs.writeFileSync(path.join(root,"SHA256SUMS"),sums);fs.appendFileSync(path.join(root,names[0]),"tampered");assert.throws(()=>verifyPayload(root,version,sha,1));
 } finally {fs.rmSync(root,{recursive:true,force:true});}
});
