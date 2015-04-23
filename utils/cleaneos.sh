for f in `eos ls /eos/cms/store/user/xiezhen/`; do
    if [[ $f =~ .csv$ ]]; then
	eos rm /eos/cms/store/user/xiezhen/${f}
    fi
done
	 
