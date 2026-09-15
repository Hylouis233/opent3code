from pathlib import Path

def replace(file, before, after):
    p=Path(file);text=p.read_text();assert text.count(before)==1,(file,before);p.write_text(text.replace(before,after))

file='apps/server/src/provider/Drivers/ExternalAcpDriver.ts'
p=Path(file);text=p.read_text();start=text.index('                    if (kind === "mcode") {\n',text.index('const started = yield* runtime.start()'));end=text.index('                    const resumeCursor = {',start)
text=text[:start]+'''                    if (kind === "mcode") {
                      // MCode's normal/plan work mode is NOT its approval policy.
                      // The separate process-scoped permissionMode option must settle to Ask.
                      const configured = yield* runtime.setConfigOption("permissionMode", "default").pipe(
                        Effect.mapError((error) => mapAcpToAdapterError(provider, input.threadId, "session/set_config_option", error)),
                      );
                      const permissionOptions = configured.configOptions.filter((option) => option.id === "permissionMode");
                      const permission = permissionOptions[0];
                      if (permissionOptions.length !== 1 || permission?.type !== "select" || permission.category !== "_permission" || permission.currentValue !== "default") {
                        return yield* invalid("session/start", "MiniMax Code did not confirm supervised permissions. No prompt was sent.");
                      }
                      if (started.sessionSetupResult.modes?.currentModeId !== "default") {
                        return yield* invalid("session/start", "This preview requires MiniMax Code's default work mode; switch out of Plan in the native CLI before resuming.");
                      }
                    }
''' + text[end:];p.write_text(text)
file='apps/server/src/provider/Drivers/ExternalAcpDriver.test.ts'
replace(file,'let mode = "bypassPermissions";','let permissionMode = "bypassPermissions";\nlet workMode = "default";')
replace(file,'const setup = () => ({sessionId:"session-1", configOptions:[{id:"mode", name:"Permissions", category:"mode", type:"select", currentValue:mode, options:[{value:"default", name:"Supervised"},{value:"bypassPermissions", name:"Full access"}]}]});','const setup = () => ({sessionId:"session-1", modes:{currentModeId:workMode,availableModes:[{id:"default",name:"Default"},{id:"plan",name:"Plan"}]}, configOptions:[{id:"permissionMode", name:"Permissions", category:"_permission", type:"select", currentValue:permissionMode, options:[{value:"default", name:"Supervised"},{value:"bypassPermissions", name:"Full access"}]}]});')
replace(file,'if (method === "session/set_config_option") { mode=params.value; return reply(id,{configOptions:setup().configOptions}); }','if (method === "session/set_mode") { workMode=params.modeId; return reply(id,{}); }\n  if (method === "session/set_config_option") { if(params.configId!=="permissionMode") return send({jsonrpc:"2.0",id,error:{code:-32602,message:"Unknown config option"}}); permissionMode=params.value; return reply(id,{configOptions:setup().configOptions}); }')
replace(file,'process.argv.includes("acp") && mode !== "default"','process.argv.includes("acp") && permissionMode !== "default"')
replace(file,'const harness = (kind: "mcode" | "dsh") =>','const harness = (kind: "mcode" | "dsh", source = fixture) =>')
replace(file,'name: kind, source: fixture','name: kind, source')
replace(file,'  for (const kind of ["mcode", "dsh"] as const) {','''  for (const [label, source] of [
    ["unsettled approval response", fixture.replace("currentValue:permissionMode", 'currentValue:"bypassPermissions"')],
    ["missing approval identity", fixture.replace('id:"permissionMode"', 'id:"unrelated"')],
    ["resumed native Plan mode", fixture.replace('let workMode = "default"', 'let workMode = "plan"')],
  ]) {
    it.effect(`mcode fails closed for ${label}`, () => Effect.gen(function* () {
      const h = yield* harness("mcode", source);
      const result = yield* h.adapter.startSession(h.start).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      assert.equal(h.adapter.listSessions().length, 0);
    }));
  }
  for (const kind of ["mcode", "dsh"] as const) {''')
# Native mode changes are distinct from permissions; document the deliberate preview boundary.
p=Path('docs/user/opent3code-preview.md');text=p.read_text();text += '\nMiniMax Code work mode and permission mode are separate: this adapter explicitly negotiates `permissionMode=default` (Ask) and verifies the returned option. A native session still in Plan mode is rejected; switch it back to Default in the CLI before resuming.\n';p.write_text(text)
