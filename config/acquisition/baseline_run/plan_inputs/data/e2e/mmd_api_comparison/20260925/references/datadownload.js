var dencat_abbrev = {
    '1': '11-499',
    '2': '500-999',
    '3': '1,000-4,999',
    '4': '5,000-9,999',
    '5': '10,000+'
};

var abbreviations = {'denominator': dencat_abbrev};

function msieversion() {
var ua = window.navigator.userAgent;
var msie = ua.indexOf("MSIE ");
if (msie > 0 || !!navigator.userAgent.match(/Trident.*rv\:11\./)) // If Internet Explorer, return true
{
    return true;
} else { // If another browser,
    return false;
}
return false;
}

function getDropDownText(name) {
return document.getElementById(name).options[document.getElementById(name).selectedIndex].text;
}

function getDropDownValue(name) {
return document.getElementById(name).options[document.getElementById(name).selectedIndex].text;
}

var mmd_data_header = function(arrData){
//console.log(arrData)
var selections = [];
var options = menuChange();
d3.selectAll("table select")[0].forEach(function(d) {
    //console.log(d.options[d.selectedIndex])
    selections.push(d.options[d.selectedIndex].text);
});
//console.log("selections")
//If the prepared dataset contains data from two years of a multiyear trend and a non-base analysis, build the output differently than for all other datasets
if (arrData[Object.keys(arrData)[0]]['Y1_V']!=undefined){
    //console.log(arrData[Object.keys(arrData)[0]])
    //Defines the first and last years of the multiyear trend, to be used in the column labels
    year1 = options.year[0].length >1 ? "20"+options.year[0].toString() : "201"+options.year[0].toString()
    year2 = options.year[1].length >1 ? "20"+options.year[1].toString() : "201"+options.year[1].toString()
    return [
        {id: 'year', v: function() { return selections[0]; }},
        {id: 'year', v: function() { return selections[1]; }},
        {id: 'geography', v: function() { return selections[2]; }},
        {id: 'measure', v: function() { return selections[3]; }},
        {id: 'adjustment', v: function() { return selections[4]; }},
        {id: 'analysis', v: function() { return selections[5]; }},
        {id: 'domain', v: function() { return selections[6]; }},
        {id: 'condition', v: function() { return '"' + selections[7] + '"'; }},
        {id: 'primary_sex', v: function() { return selections[8]; }},
        {id: 'primary_age', v: function() { return selections[9]; }},
        {id: 'primary_dual', v: function() { return selections[11]; }},

        {id: 'fips', v: function(d) { return d; }},
        {id: 'county', v: function(d) { return d < 100 ? 'Statewide' : namesById.get(d); }},
        {id: 'state', v: function(d) { return d < 100 ? namesById.get(d) : namesById.get(Math.floor(d/1000)); }},
        {id: 'urban', v: function(d) { if (language === "english") {return urbanById.has(d) ? 'Urban' : 'Rural';} else {return urbanById.has(d) ? 'Urbano' : 'Rural';}
    }},
        {id: 'primary_race', v: function() { return selections[10]; }},
        {id: year1+'_primary_value', v: function(d) { return arrData[d]['Y1_V1']; }},
        {id: year2+'_primary_value', v: function(d) { return arrData[d]['Y2_V1']; }},
        {id: 'primary_eligibility', v: function() { return selections[12]; }},
        {id: year1+'_primary_denominator', v: function(d) {
            var _ = '"' + dencat_abbrev[
                'Y1_D1' in arrData[d] ?
                    arrData[d]['Y1_D1']
                :
                    arrData[d]['Y1_D']
                ] + '"';

            if (typeof(_) === undefined) { return 'N/A' } else { return _; };
        }},
        {id: year2+'_primary_denominator', v: function(d) {
            var _ = '"' + dencat_abbrev[
                'Y2_D1' in arrData[d] ?
                    arrData[d]['Y2_D1']
                :
                    arrData[d]['Y2_D']
                ] + '"';

            if (typeof(_) === undefined) { return 'N/A' } else { return _; };
        }},
        {id: 'comparison_sex', v: function() { return selections[13]; }},
        {id: 'comparison_age', v: function() { return selections[14]; }},
        {id: 'comparison_dual', v: function() { return selections[16]; }},
        {id: 'comparison_race', v: function() { return selections[15]; }},
        {id: 'comparison_eligibility', v: function(d) { return selections[17]; }},
        {id: year1+'_comparison_value', v: function(d) { return arrData[d]['Y1_V2']; }},
        {id: year2+'_comparison_value', v: function(d) { return arrData[d]['Y2_V2']; }},
        {id: year1+'_comparison_denominator', v: function(d) {
            var _ = '"' + dencat_abbrev[
                arrData[d]['Y1_D2']
            ] + '"';

            if (typeof(_) === undefined) { return 'N/A' } else { return _; };
        }},
        {id: year2+'_comparison_denominator', v: function(d) {
            var _ = '"' + dencat_abbrev[
                arrData[d]['Y2_D2']
            ] + '"';

            if (typeof(_) === undefined) { return 'N/A' } else { return _; };
        }},
        {id: year1+'_analysis_value', v: function(d) { return arrData[d]['Y1_V']; }},
        {id: year2+'_analysis_value', v: function(d) { return arrData[d]['Y2_V']; }},
        {id: 'trend', v: function(d) { return arrData[d]['V_trend']; }},
        ];
}
else{
return  [
{id: 'year', v: function() { return selections[0]; }},
{id: 'year', v: function() { return selections[1]; }},
{id: 'geography', v: function() { return selections[2]; }},
{id: 'measure', v: function() { return selections[3]; }},
{id: 'adjustment', v: function() { return selections[4]; }},
{id: 'analysis', v: function() { return selections[5]; }},
{id: 'domain', v: function() { return selections[6]; }},
{id: 'condition', v: function() { return '"' + selections[7] + '"'; }},
{id: 'primary_sex', v: function() { return selections[8]; }},
{id: 'primary_age', v: function() { return selections[9]; }},
{id: 'primary_dual', v: function() { return selections[11]; }},



{id: 'fips', v: function(d) { return d; }},
{id: 'county', v: function(d) { return d < 100 ? 'Statewide' : namesById.get(d); }},
{id: 'state', v: function(d) { return d < 100 ? namesById.get(d) : namesById.get(Math.floor(d/1000)); }},
{id: 'urban', v: function(d) { if (language === "english") {return urbanById.has(d) ? 'Urban' : 'Rural';} else {return urbanById.has(d) ? 'Urbano' : 'Rural';}
}},
{id: 'primary_race', v: function() { return selections[10]; }},
{id: 'primary_value', v: function(d) { return arrData[d]['V1']; }},
{id: 'primary_eligibility', v: function() { return selections[12]; }},
{id: 'primary_denominator', v: function(d) {
    var _ = '"' + dencat_abbrev[
        'D1' in arrData[d] ?
            arrData[d]['D1']
        :
            arrData[d]['D']
        ] + '"';

    if (typeof(_) === undefined) { return 'N/A' } else { return _; };
}},
{id: 'comparison_sex', v: function() { return selections[13]; }},
{id: 'comparison_age', v: function() { return selections[14]; }},
{id: 'comparison_dual', v: function() { return selections[16]; }},
{id: 'comparison_race', v: function() { return selections[15]; }},
{id: 'comparison_eligibility', v: function(d) { return selections[17]; }},
{id: 'comparison_value', v: function(d) { return arrData[d]['V2']; }},
{id: 'comparison_denominator', v: function(d) {
    var _ = '"' + dencat_abbrev[
        arrData[d]['D2']
    ] + '"';

    if (typeof(_) === undefined) { return 'N/A' } else { return _; };
}},
{id: 'analysis_value', v: function(d) { return arrData[d]['V']; }}


];}
}

var mmd_data_header_column_ids = function(){

var columns = ['population','year', 'geography', 'measure', 'adjustment', 'analysis','domain',
             'condition', 'primary_sex', 'primary_age', 'primary_dual', 'fips'];

             var options = menuChange();

                 if (options.geography === 'c') {
                     columns = columns.concat(['county', 'state', 'urban']);
                 } else {
                     columns = columns.concat(['state']);
                 };

                 if (options.analysis === 'base') {
                     columns = columns.concat(['primary_race', 'primary_eligibility', 'primary_denominator', 'analysis_value']);
                 } else {
                    //If multiyear trend is sleected and analysis is not a base analysis, set column headers differently than all other selections
                    if (options.year.constructor.name==="Array"){
                        year1 = options.year[0].length >1 ? "20"+options.year[0].toString() : "201"+options.year[0].toString()
                        year2 = options.year[1].length >1 ? "20"+options.year[1].toString() : "201"+options.year[1].toString()
                        columns = columns.concat(['primary_race', year1+"_primary_value", year2+"_primary_value",'primary_eligibility', year1+'_primary_denominator', year2+'_primary_denominator',
                                 'comparison_sex', 'comparison_age', 'comparison_dual','comparison_race', 'comparison_eligibility', year1+'_comparison_value',year2+'_comparison_value',
                                 year1+'_comparison_denominator',year2+'_comparison_denominator',year1+'_analysis_value', year2+'_analysis_value', 'trend']);
                    } 
                    else{
                        columns = columns.concat(['primary_race', 'primary_value', 'primary_eligibility', 'primary_denominator',
                                 'comparison_sex', 'comparison_age', 'comparison_dual','comparison_race', 'comparison_eligibility', 'comparison_value','comparison_denominator',
                                 'analysis_value']);
                    }
                 };
                 
             return columns;

           }

function get_mmd_array_data_from_object(JSONData){

    if (!JSONData) {
    return;
  }
  
  var arrData = typeof JSONData != 'object' ? JSON.parse(JSONData) : JSONData;

  //console.log("arrData")
  //console.log(arrData)
  var array_of_row_arrays = [];

  var header = mmd_data_header(arrData);
  //console.log("header")
  //console.log(header)
  var columns = mmd_data_header_column_ids();

  var options = menuChange();

  for (var key in arrData) {

      if (key < 1000 && options.geography === 'c') { continue; };

      if (isNaN(key)) { continue; };

      var row_array = [];

      for (var i = 0; i < header.length; i++) {
          if (columns.indexOf(header[i].id) > -1) {
              row_array.push(header[i].v(key));
          }
      };
      array_of_row_arrays.push(row_array);

  };
  //console.log(array_of_row_arrays[0]);
  return array_of_row_arrays;
}

function download_csv(array_of_row_arrays, fileName, ShowLabel, column_label_func) {
//console.log(array_of_row_arrays)
if (!array_of_row_arrays) {
  console.error("CSV is empty string; Invalid data");
  return;
}
//Don't need to pass arrData in this case, only the headers' ids are used
//Sets column headers to English or Spanish, depending on the language selected in the tool 
var language = get_or_set_language_cookie();
if (language === "english") {
    var columns = (column_label_func || mmd_data_header_column_ids)();
}
else{
    var columns = ['población', 'año', 'geografía', 'medida', 'ajuste', 'análisis','dominio',
    'condición', 'sexo_principal', 'edad_principal', 'doble elegibilidad_principal', 'Normas federales de procesamiento de información '];
    var options = menuChange();
//To do: Finish translation
                 if (options.geography === 'c') {
                     columns = columns.concat(['condado', 'estado', 'urbano']);
                 } else {
                     columns = columns.concat(['estado']);
                 };

                 if (options.analysis === 'base') {
                     columns = columns.concat(['raza_principal', 'elegibilidad_principal', 'denominador_principal', 'análisis_valor']);
                 } else {
                     columns = columns.concat(['raza_principal', 'principal_valor', 'elegibilidad_principal', 'denominador_principal',
                                 'comparación_sexo', 'comparación_edad', 'comparación_doble elegibilidad','comparación_raza', 'comparación_elegibilidad', 'comparación_valor','denominador_comparación',
                                 'análisis_valor']);
                 };
}
//console.log(columns);
array_of_row_strings = [columns.join(',')].concat(array_of_row_arrays.map(function(row_array){
        return row_array.join(',');
}));

var CSV = array_of_row_strings.join('\r\n');

if (CSV == '') {
    console.error("CSV is empty string; Invalid data");
    return;
}
if (window.navigator.msSaveOrOpenBlob) {
    var blob = new Blob([CSV], { type: 'text/csv;charset=utf-8;' });
    navigator.msSaveBlob(blob, 'mmd_data.csv');
} else if (msieversion()) {
    var IEwindow = window.open();
    IEwindow.document.write('sep=,\r\n' + CSV);
    IEwindow.document.close();
    IEwindow.document.execCommand('SaveAs', true, fileName);
    IEwindow.close();
} else {
  //Using text instead of application so it doesn't open in Excel
    var uri = 'data:text/csv;charset=utf-8,' + escape(CSV);
    var link = document.createElement("a");
    link.href = uri;
    link.style = "visibility:hidden";
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}
}
