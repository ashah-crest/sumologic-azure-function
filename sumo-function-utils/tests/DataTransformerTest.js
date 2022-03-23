/**
 * Created by duc on 6/30/17.
 */


import { Transformer } from '../lib/datatransformer.js';
import { expect as _expect, should } from 'chai';
var expect = _expect;
should();


describe('DataTransformerTest',function () {
    let myTransformer = new Transformer();
    let testInput;
    let testMessageCount = 10;
    let testBlobCount = 3;

    beforeEach( function(){
        testInput=[];
        for (let j = 0; j<testBlobCount; j++ ) {
            let tmp_buff = [];
            for (let i = 0; i < testMessageCount; i++) {
                let sourceCat = Math.ceil(Math.random() * 10);
                tmp_buff.push({_sumo_metadata: {category: sourceCat}, value: i});
            }
            testInput.push({records:tmp_buff});
        }
    });

    it('it should unpack a single input blob properly',function() {
        expect(myTransformer.azureAudit(testInput[0]).length).to.equal(testMessageCount);
    });

    it('it should unpack an array input blob properly',function() {
        expect(myTransformer.azureAudit(testInput).length).to.equal(testMessageCount*testBlobCount);
    });

});

