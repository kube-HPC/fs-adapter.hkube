const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const { expect } = chai;
const moment = require('moment');
const path = require('path');
const fs = require('fs-extra');
const baseDir = 'storage/nfs/test/';
const uuid = require('uuid/v4');
const adapter = require('../lib/fs-adapter');
const DIR_NAMES = {
    HKUBE: 'data/hkube',
    HKUBE_RESULTS: 'data/hkube-results',
    HKUBE_METADATA: 'data/hkube-metadata',
    HKUBE_STORE: 'data/hkube-store',
    HKUBE_EXECUTION: 'data/hkube-execution'
};
const DateFormat = 'YYYY-MM-DD';

describe('fs-adapter', () => {
    before(async () => {
        const options = {
            baseDirectory: baseDir
        };
        await adapter.init(options, null, DIR_NAMES, true);
    });
    describe('put', () => {
        it('put and get same value', async () => {
            const jobId = uuid();
            const link = await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test' });
            const res = await adapter.get(link);
            expect(res).to.equal('test');
        });
        it('put and get results same value', async () => {
            const link = await adapter.put({ Path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), Data: 'test-result' });
            const res = await adapter.get(link);
            expect(res).to.equal('test-result');
        });
        it('put and get results null', async () => {
            const link = await adapter.put({ Path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), Data: null });
            const res = await adapter.get(link);
            expect(res).to.equal(null);
        });
        it('put and get results []', async () => {
            const link = await adapter.put({ Path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), Data: [] });
            const res = await adapter.get(link);
            expect(res).to.deep.equal([]);
        });
        it('put and get results 33', async () => {
            const link = await adapter.put({ Path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), Data: 33 });
            const res = await adapter.get(link);
            expect(res).to.equal(33);
        });
        it('put and delete', async () => {
            const link = await adapter.put({ Path: path.join(DIR_NAMES.HKUBE_RESULTS, moment().format(DateFormat), uuid()), Data: 33 });
            const res = await fs.pathExists(path.join(baseDir, link.Path));
            expect(res).to.equal(true);

            await adapter.delete(link);
            const res1 = await fs.pathExists(path.join(baseDir, link.Path));
            expect(res1).to.equal(false);
        });
        it('put and list', async () => {
            const jobId = uuid();
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test1' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test2' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test3' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test4' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test5' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test6' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test7' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test8' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test9' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test10' });
            await adapter.put({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId, uuid()), Data: 'test11' });

            const res = await adapter.list({ Path: path.join(DIR_NAMES.HKUBE, moment().format(DateFormat), jobId) });
            expect(res.length).to.equal(11);
        });
    });
    after(() => {
        Object.values(DIR_NAMES).forEach(dir => fs.removeSync(path.join(baseDir, dir)));
    });
});
