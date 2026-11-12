//Adjusts scale to a predetermined number of categories/bins, collapsing the bins that contain identical values
function adjustedScale() {

    var domain = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        range = [0, 1, 2, 3, 4, 5, 6, 7, 8],
        convert = {},
        bins = null,
        threshold = 3;

    function scale(v) {

        /* Quantile Returned is Limited to >= lower and
                                            < upper  bounds, specifically */
        for (var i = 0; i < bins.length; i++) {
            var cr = range[convert[i]];
            if (bins[i][0] === null && v < bins[i][1]) { return cr; }
            else if (bins[i][1] === null && v >= bins[i][0]) { return cr; }
            else if (v >= bins[i][0] && v < bins[i][1]) { return cr; }
            else if (v === bins[i][0] && v === bins[i][1]) { return cr; }
        }

    }

    scale.domain = function (dom) {

         //Sorts domain in ascending order
         //Remove non-numeric values, as well
        if (!arguments.length) { return domain; }
        domain = dom
            .filter(function(d) { return isFinite(d); })
            .sort(function (a, b) { return a - b; });

        /* Count Unique Values in Domain */
        var _uvals = domain.filter(function (x, i) { return domain.indexOf(x) === i; });
        var _u = _uvals.length;
        //console.log('Unique values: ' + _uvals);
        //console.log('Unique values length: ' + _uvals.length);

        /* Initial Bin Creation */
        //Old code: _u >= range.length
        //console.log(Math.max.apply(Math, _uvals)>1)
        if (_u >= range.length) {

            bins = new Array(range.length);
            //Create scaling array
            var inds = [];
            var off = domain.length / range.length;
            //console.log('Domain length: '+domain.length);
            //console.log('Range length: '+range.length);
            //console.log('Offset: '+off);
            for (var i = 1; i < range.length; i++) {
                inds.push({ 'index': Math.floor(off * i), 'offset': off * i - Math.floor(off * i) });
            }
            //console.log('Inds length: '+inds.length);
            //console.log(inds);
            var interp = function (d, i) {
                return d[i.index] + (d[i.index + 1] - d[i.index]) * i.offset;
            };
            //Find mode in an array
            var modeValue = function(domain){
                domainArray = []
                for (var i =1; i<domain.length; i++){
                    domainArray.push(domain[i])
                }
                return domainArray.sort(function(a,b){return domainArray.filter(function(value){return value===a}).length - domainArray.filter(function(value){return value===b}).length}).pop();
            };
            //console.log('Mode: '+modeValue(domain));
            var pastIndex = 0
            //While last values in scaling array are equal to the mode of the domain
            while (interp(domain, inds[inds.length - 1])==modeValue(domain)){
                var pastIndex = pastIndex + Math.floor(off * (range.length-1))
                //console.log('Past Index: ' + pastIndex);
                var off=off/range.length;
                inds = [];
                //console.log(off);
                //inds.push(oldInds);
                //console.log('New lower inds: '+inds);
                for (var i = 1; i < range.length; i++) {
                    inds.push({ 'index': Math.floor(pastIndex+off * i), 'offset': (pastIndex+off * i) - Math.floor(pastIndex+off * i) });
                }
                //console.log(inds[inds.length - 1,1]);
                //console.log(interp(domain, inds[inds.length - 1]));
            }
            
            //console.log(inds);
            bins[0] = [null, interp(domain, inds[0])];
            
            //Fill scaling array with domain values 
            for (var i = 0; i < inds.length - 1; i++) {
                bins[i + 1] = [interp(domain, inds[i]), interp(domain, inds[i + 1])]
            }

            bins[bins.length - 1] = [interp(domain, inds[inds.length - 1]), null];
            
            //console.log('Bins length: '+bins.length);

           // if (bins.length>_u){/* Bin Collapse - Squash Adjacent Bins */
            var quashed = [].concat.apply([], bins);
            var quashedvals = [quashed[0]];
            for (var i = 0; i < quashed.length - 1; i++) {
                if (quashed[i] !== quashed[i + 1]) { quashedvals.push(quashed[i + 1]); }
            }

            var newbins = [];
            for (var i = 0; i < quashedvals.length - 1; i++) {
                //newbins.push(quashedvals.slice(i, i + 2));
                qv=quashedvals.slice(i,i+2)
                //console.log(qv)
                //Rounds breakpoints in legend to whole numbers, but only if maximum value on map >1 (excludes measures where all values are <1). Also excludes Particulate matter SDOH measure
                if(Math.max.apply(Math, _uvals)>1 && document.getElementById("measure").value!='3'){
                if (i===0){
                    qv[1]=Math.floor(qv[1])*0.999999
                }
                else if (i==(quashedvals.length-2)){
                    //console.log(qv[0])
                    if (qv[0]<newbins[i-1][1]){
                        qv[0]=Math.round(newbins[i-1][1])
                    }
                    else{
                        qv[0]=Math.floor(qv[0])
                    }
                }
                else{
                    //Checks if difference between breakpoints is at least 1. If not, sets breakpoints so that difference is 1
                    if (qv[1]-qv[0]<1.0000000001){
                        qv[0]=Math.floor(qv[0])
                        qv[1]=(qv[0]+1)*0.999999
                        //console.log(qv)

                    }else{
                        qv[0]=Math.floor(qv[0])
                        qv[1]=Math.floor(qv[1])*0.999999
                    }
                }}
                newbins.push(qv)
            
            }

            bins = newbins;

        } else {

            /* Simply place values into their own group */
            bins = new Array(_u);
            for (var i = 0; i < _uvals.length; i++) {
                bins[i] = [_uvals[i], _uvals[i]];
            }

        }
        
        if (bins.length === range.length) {
            for (var i = 0; i < bins.length; i++) { convert[i] = i; }
        } else {
            var space = Math.floor(range.length / bins.length)
            if (space > 1) {
                for (var i = 0; i < bins.length; i++) { convert[i] = space * i; }
            } else {
                var diff = range.length - bins.length,
                    i = range.length - 1,
                    j = bins.length - 1;
                while (j >= 0) {
                    convert[j] = i
                    j--; i--;
                    if (diff) { i--; diff-- }
                }
            }                    
        }

        return scale;
    }

    scale.range = function (value) {
        if (!arguments.length) { return range; }
        if (value !== range) {
            range = value;
            return scale.domain(domain);
        }
        return scale;
    }

    scale.bins = function (value) {
        if (!arguments.length) { return bins; }
        bins = value;
        return scale;
    }

    scale.convert = function (value) {
        if (!arguments.length) { return convert; }
        convert = value;
        return scale;
    }

    return scale;
}
//Adjusts scale for state visualization only
function adjustedScaleState() {

var domain = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
range = [0, 1, 2, 3, 4, 5, 6, 7, 8],
convert = {},
bins = null,
threshold = 3;

function scale(v) {

/* Quantile Returned is Limited to >= lower and
                    < upper  bounds, specifically */
for (var i = 0; i < bins.length; i++) {
var cr = range[convert[i]];
if (bins[i][0] === null && v < bins[i][1]) { return cr; }
else if (bins[i][1] === null && v >= bins[i][0]) { return cr; }
else if (v >= bins[i][0] && v < bins[i][1]) { return cr; }
else if (v === bins[i][0] && v === bins[i][1]) { return cr; }
}
}

scale.domain = function(dom) {
if (!arguments.length) { return domain; }
domain = dom.sort(function(a,b) { return a - b; });

/* Count Unique Values in Domain */
var _uvals = domain.filter(function(x,i) { return domain.indexOf(x) === i; });
var _u = _uvals.length;
//console.log(_uvals)

/* Initial Bin Creation */
if (_u == range.length+1){
    //If there are only 6 unique values, create 5 categories 
    bins = new Array(_u-1);
    for (var i = 0; i < _u-1; i++) {
        if (i==0){
            bins[i] = [null, _uvals[i+1]];
        }
        else if (i==_uvals.length-2){
            bins[i] = [_uvals[i], null];
        }
        else{
            bins[i] = [_uvals[i], _uvals[i+1]];
        }
    }
}
else if (_u > range.length) {
//If there are more unique values than categories, group values into categories
bins = new Array(range.length);

var inds = [];
var off = domain.length/range.length;
for (var i = 1; i < range.length; i++) {
    inds.push({'index': Math.floor(off*i), 'offset': off*i - Math.floor(off*i)} );
}
//console.log(inds)
var interp = function(d,i) {
    return d[i.index] + (d[i.index+1]-d[i.index])*i.offset;
};

bins[0] = [null, interp(domain,inds[0])];

for (var i = 0; i < inds.length - 1; i++) {
    bins[i+1] = [interp(domain,inds[i]),interp(domain,inds[i+1])]
    //console.log(bins[i+1])
}

bins[bins.length-1] = [interp(domain,inds[inds.length-1]),null];
//console.log(bins)
/* Bin Collapse - Squash Adjacent Bins */
var quashed = [].concat.apply([],bins);
var quashedvals = [quashed[0]];
for (var i = 0; i < quashed.length - 1; i++) {
    if (quashed[i] !== quashed[i+1]) { quashedvals.push(quashed[i+1]); }
}

var newbins = [];
//console.log(quashedvals)
for (var i = 0; i < quashedvals.length - 1; i++) {
    //newbins.push(quashedvals.slice(i,i+2));
    qv=quashedvals.slice(i,i+2)
                //console.log(qv)
                //Rounds breakpoints in legend to whole numbers
                if(Math.max.apply(Math, _uvals)>1 ){
                if (i===0){
                    qv[1]=Math.floor(qv[1])*0.999999
                }
                else if (i==(quashedvals.length-2)){
                    if (qv[0]<newbins[i-1][1]){
                        qv[0]=Math.round(newbins[i-1][1])
                    }
                    else{
                        qv[0]=Math.floor(qv[0])
                    }
                }
                else{
                    //Checks if difference between breakpoints is at least 1. If not, sets breakpoints so that difference is 1
                    if (qv[1]-qv[0]<1.0000000001){
                        qv[0]=Math.floor(qv[0])
                        qv[1]=(qv[0]+1)*0.999999
                        //console.log(qv)

                    }else{
                        qv[0]=Math.floor(qv[0])
                        qv[1]=Math.floor(qv[1])*0.999999
                    }
                }}
                    newbins.push(qv)
                  
}
//console.log(newbins)
bins = newbins;

} else {
/* If there are fewer unique values than categories, simply place values into their own group */
bins = new Array(_u);
for (var i = 0; i < _uvals.length; i++) {
    bins[i] = [_uvals[i], _uvals[i]];
}
//console.log("bins")
//console.log(bins)
}
//console.log(bins)
//Removes bins with duplicate ranges
unduplicatedBins = [];
unduplicatedBins.push(bins[0])
for (i = 1; i<bins.length; i++){
    if(bins[i].toString()===bins[i-1].toString()){
        
    }
    else{
        unduplicatedBins.push(bins[i])
    }
}

bins = unduplicatedBins;

if (bins.length === range.length) {
for (var i = 0; i < bins.length; i++) { convert[i] = i; }
} 
else {
var space = Math.floor(range.length / bins.length)
if (space > 1) {
    for (var i = 0; i < bins.length; i++) { convert[i] = space*i; }
} else {
    var diff = range.length - bins.length,
        i = range.length - 1,
        j = bins.length - 1;
    while (j >= 0) {
        convert[j] = i
        j--; i--;
        if (diff) { i--; diff-- }
    }
}
}

return scale;
}

scale.range = function (value) {
if (!arguments.length) { return range; }
if (value !== range) {
range = value;
return scale.domain(domain);
}
return scale;
}

scale.bins = function (value) {
if (!arguments.length) { return bins; }
bins = value;
return scale;
}

scale.convert = function (value) {
if (!arguments.length) { return convert; }
convert = value;
return scale;
}

return scale;
}