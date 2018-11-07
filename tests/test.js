const { expect } = require('chai');
const adapter = require('../lib/fs-adapter');
const fs = require('fs-extra');
const baseDir = 'storage/test/';
const uuid = require('uuid/v4');

describe('fs-adapter', () => {
    before(async () => {
        const options = {
            baseDirectory: baseDir
        };
        await adapter.init(options, null, true);
    });
    describe('put', () => {
        it('put and get same value', async () => {
            const jobId = uuid();
            const link = await adapter.put({ jobId, taskId: uuid(), data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('put and get results same value', async () => {
            const link = await adapter.putResults({ jobId: uuid(), data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('put and get results null', async () => {
            const link = await adapter.putResults({ jobId: uuid(), data: null });
            const res = await adapter.get(link);
            expect(res).to.equal(null);
        });
        it('put and get results []', async () => {
            const link = await adapter.putResults({ jobId: uuid(), data: [] });
            const res = await adapter.get(link);
            expect(res).to.deep.equal([]);
        });
        it('put and get results 33', async () => {
            const link = await adapter.putResults({ jobId: uuid(), data: 33 });
            const res = await adapter.get(link);
            expect(res).to.equal(33);
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
