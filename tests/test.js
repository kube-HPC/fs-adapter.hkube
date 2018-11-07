const { expect } = require('chai');
const adapter = require('../lib/fs-adapter');
const fs = require('fs-extra');
const baseDir = 'storage/test/';

describe('fs-adapter', () => {
    before(async () => {
        const options = {
            baseDirectory: baseDir
        };
        await adapter.init(options, null, true);
    });
    describe('put', () => {
        it('put and get same value', async () => {
            const link = await adapter.put({ jobId: Date.now(), taskId: 'task-1', data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('put and get results same value', async () => {
            const link = await adapter.putResults({ jobId: 'same-value-test', data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
    });
    describe('jobPath', () => {
        it('jobPath', async () => {
            await adapter.jobPath({ jobId: 'same-value-test', taskId: 'task-1', data: 'test' });
        });
    });
    after(() => {
        fs.removeSync(baseDir);
    });
});
