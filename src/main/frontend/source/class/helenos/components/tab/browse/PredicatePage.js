/* ************************************************************************
Copyright:
  2012 Tomek Kuprowski
License:
  GPLv2: http://www.gnu.org/licences/gpl.html
Authors:
  Tomek Kuprowski (tomekkuprowski at gmail dot com)
 ************************************************************************ */
qx.Class.define("helenos.components.tab.browse.PredicatePage",
{
    extend : helenos.components.tab.browse.AbstractPage,
 
    construct : function(ksName, cfName)
    {
        this.base(arguments, ksName, cfName);
    },
    
    statics : {
        PARTITION_KEY: 'PARTITION_KEY',
        CLUSTERING_KEY: 'CLUSTERING_KEY'
    },
    
    members :
    {
        __clusteringKeys: null,
        __partitionKeys: null,
        __rowCountTF : null,
        __keyMap : null,
        
        _getSplitPaneOrientation : function() {
            return 'vertical';
        },
        
        _getIconPath : function() {
            return 'icon/16/apps/office-spreadsheet.png';
        },
        
        _performSearch  : function(e) {
        	this._queryObj = new helenos.model.CombinedQuery();
            this._queryObj.setParamList ( new qx.type.Array());
            var allKeys = new qx.type.Array();
            allKeys.append(this.__partitionKeys);
            allKeys.append(this.__clusteringKeys);
        	for( i = 0; i < allKeys.length;i++){
        		var array = this.__keyMap.get(allKeys[i].name);
        		var param = new helenos.model.QxCombinedQueryParam();
        		param.setName(allKeys[i].name);
        		param.setValidationClass(this._queryObj._findParamClass(allKeys[i].validationClass));
        		param.setComparisonOperator(array[0].getSelection()[0].getLabel());
        		param.setFrom(array[1].getValue());
        		param.setTo(array[2].getValue());
        		this._queryObj.getParamList().push(param);
        	}
            var consistencyLevel = this._consistencyLevelSB.getSelection()[0].getLabel();
            this._queryObj.prepareQuery(this._cfDef, consistencyLevel);
            this._queryObj.setRowCount(this.__rowCountTF.getValue());

            var jsonQuery = qx.util.Serializer.toNativeObject(this._queryObj, null, null);
            var result = null;
            result = helenos.util.RpcActionsProvider.queryCombined(this._cfDef, jsonQuery);
            if( result.length == 0){
            	(new dialog.Alert({
                    "message" : 'No results found.',
                    'image' : 'icon/48/status/dialog-information.png'
                })).set({width : 350}).show();
            }
            return result;
        },
        _getResultsPane : function() {
            var pane = this.base(arguments);
            return pane;
        },
        
        _getCriteriaPane : function() {
            
            var container = new qx.ui.container.Composite(new qx.ui.layout.HBox(1).set({alignX : 'left'}));
            container.setAppearance('criteria-pane');
            container.add(this.__buildKeysGB());
            
            var searchContainer = new qx.ui.container.Composite(new qx.ui.layout.VBox(2).set({alignX : 'left', spacing: 60}));
            searchContainer.add(this.__buildConsistencyLevelGB());
            
            var searchContainer2 = new qx.ui.container.Composite(new qx.ui.layout.VBox(2).set({alignY : 'bottom'}));
            searchContainer2.add(this._getSearchButton());
            searchContainer2.add(this.__buildResetButton());
            searchContainer.add(searchContainer2);
            
            container.add(searchContainer);
            
            var pane = new qx.ui.container.Scroll();
            pane.setWidth(180);
            pane.add(container);
            return pane;
        },
        
        __buildResetButton : function() {
            var button = new qx.ui.form.Button('Reset', 'icon/16/actions/edit-clear.png');
            button.addListener('execute', this.__resetSearchForm, this);
            return button;
        },
        
        __resetSearchForm : function(e) {
            this._resetter.reset();
        },
        
        __buildConsistencyLevelGB : function() {
            this._initConsistencyLevelSB();
            
            var consLevelGB = new helenos.ui.GroupBoxV(this.tr('consistency.level'));
            consLevelGB.add(this._consistencyLevelSB);

            return consLevelGB;
        },
        
        __isSuperColumnMode : function() {
            return this._cfDef.columnType == 'Super';
        },
        
        __buildKeysGB : function() {          
            var keysGB = new helenos.ui.GroupBoxV('Keys');
            keysGB.add(this.__buildKeysBox());
            
            //keysGB.add(new qx.ui.basic.Label('Key mode:'));
            //keysGB.add(this.__buildKeyModeBG());
            
            return keysGB;
        },
        
        __buildKeysBox : function(){
        	 this.__partitionKeys = [];
        	 this.__clusteringKeys = [];
			 for(var i = 0; i < this._cfDef.columnMetadata.length; i++){
				 if( this._cfDef.columnMetadata[i].keyType == this.self(arguments).PARTITION_KEY){
					 this.__partitionKeys.push(this._cfDef.columnMetadata[i]);
				 }
				 if( this._cfDef.columnMetadata[i].keyType == this.self(arguments).CLUSTERING_KEY){
					 this.__clusteringKeys.push(this._cfDef.columnMetadata[i]);
				 }
			 }
			 var _keysCP = new qx.ui.container.Composite(new qx.ui.layout.VBox(5)).set({padding : 5});
			 _keysCP.add (new qx.ui.basic.Label( 'Partition keys:'));
			 this.__keyMap = new Map();
			 for(var i = 0; i < this.__partitionKeys.length; i++){
				 var keyCP = this.__createKeyGroupField(this.__partitionKeys[i], false);
				 _keysCP.add(keyCP);
			 }
			 if( this.__clusteringKeys.length > 0){
				 _keysCP.add (new qx.ui.basic.Label( 'Clustering keys:'));
			 }
			 for(var i = 0; i < this.__clusteringKeys.length; i++){
				 var keyCP = this.__createKeyGroupField(this.__clusteringKeys[i],i == (this.__clusteringKeys.length-1));
				 _keysCP.add(keyCP);
			 }
			 _keysCP.add(new qx.ui.basic.Label('Max keys:'));
			 var c = new qx.ui.container.Composite(new qx.ui.layout.HBox(1));
			 this.__rowCountTF = new qx.ui.form.TextField().set({filter : /[0-9]/, value : '100', required : true, width : 120});
			 c.add(this.__rowCountTF);
			 _keysCP.add(c);
			 return _keysCP;
        },
        
        __createKeyGroupField : function(keyDef, enabled){
        	var keyCP = new qx.ui.container.Composite(new qx.ui.layout.HBox(5)).set({padding : 0});
			keyCP.add (new qx.ui.basic.Label( keyDef.name +':'));
			
			var comparison = this.__createNewComparisonBox();
			comparison.setEnabled(enabled);
			keyCP.add(comparison);
			
			var keyFrom = new helenos.ui.TextField(keyDef.validationClass).set({required : true})
			this._addToResetter(keyFrom);
			this._addToDisabler(keyFrom);
			this._addToValidator(keyFrom);
			keyCP.add(keyFrom);
			
			var keyTo = new helenos.ui.TextField(keyDef.validationClass).set({required : false})
			this._addToResetter(keyTo);
			this._addToDisabler(keyTo);
			keyTo.hide();
			keyCP.add(keyTo);
			
			var paramArray = [];
			paramArray.push(comparison);
			paramArray.push(keyFrom);
			paramArray.push(keyTo);
			this.__keyMap.set(keyDef.name, paramArray);
			
			return keyCP;
        },
        __createNewComparisonBox : function(){
        	var comparisonBox = new qx.ui.form.SelectBox();
            for (var i=0; i < helenos.util.Constants.comparisonTypes.length; i++ ) {
                var item = helenos.util.Constants.comparisonTypes[i];
                comparisonBox.add(new qx.ui.form.ListItem(item,null,item));
            }
            comparisonBox.addListener('changeSelection', this.__onComparisonBoxChange, this);
            return comparisonBox;
        },
        __onComparisonBoxChange : function(e){
        	
        	for( i = 0; i < this.__clusteringKeys.length;i++){
        		var array = this.__keyMap.get(this.__clusteringKeys[i].name);
        		if( qx.lang.Object.equals(array[0],e.getTarget())){
        			if( e.getTarget().getSelection()[0].getLabel() == 'between'){
        				array[2].show();
        			}
        			else{
        				array[2].hide();
        				}
        		}
        	}
        }
    }
});