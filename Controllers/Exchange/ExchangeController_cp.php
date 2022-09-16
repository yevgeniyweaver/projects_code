<?php

namespace App\Http\Controllers\Exchange;

use App\DTO\input\PrebidSourceDTO;
use App\Http\Controllers\Controller;
use App\Http\Requests\ScanLimitsUpdateRequest;
use App\Models\BaseModel;
use App\Models\BidResponse;
use App\Models\CheckTrafSSPExclude;
use App\Models\DspSettingsPrebidSourceMapper;
use App\Models\ListCustomSSP;
use App\Models\ListDSP;
use App\Models\ListSSP;
use App\Models\PrebidjsStats;
use App\Models\PrebidPartners;
use App\Models\Project;
use App\Models\ScanLimits;
use App\Models\SettingsDSP;
use App\Models\SettingsDspPrebid;
use App\Models\SettingsEmailReport;
use App\Models\SettingsScanDiscrepReport;
use App\Models\SettingsSSP;
use App\Models\SspPlacementsSettings;
use App\Services\CustomSspService;
use App\Services\SspPlacementService;
use DateTime;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Cookie;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use JsonSchema\Constraints\Constraint;
use PhpOffice\PhpSpreadsheet\Spreadsheet;

class ExchangeController extends Controller
{

    private $minCountDayToGraph = 7;
    const COUNT_PAGINATION = 5;

    public $typeReport = [
        'general' => "General",
        "detailed" => "Detailed",
        "tmtAlerts" => "TMT Alerts",
    ];

    private $arrayAdvenueCustomSspBlack = [114, 148, 149, 168, 329, 271];
    private $customSspService;
    private $sspPlacementService;

    private $actionAjax = [
        'setCustomSspId',
        'setCustomCompany',
        'setCustomConfidential',
        'setCustomSspExclude',
        'changeRoleCustomSsp',
        'setCustomSspDomain',
        'downloadSellersJson',
        'getInvalidSettings',
        'duplicateCustomId',
        'setSourceGetpublica',
        'setCustomIdCustomSsp',
        'updateCustomSsp',
        'updateSspPlacement'
    ];

    public function __construct()
    {
        //$this->middleware('auth');

        if (in_array(app()->project->name, [Project::BIZZ_CLICK])) {
            $this->typeReport['prepayAlert'] = 'Prepay Alert';
        }

        if (app()->project->name === Project::ACUITY) {
            $this->typeReport['publisher'] = 'Publisher';
            $this->typeReport['trafquality'] = 'Traffic Quality';
            $this->typeReport['scanDiscrep'] = 'Scan Discrepancy';
            $this->typeReport['profitReport'] = 'Profit Report';
        }

        $this->customSspService = new CustomSspService();
        $this->sspPlacementService = new SspPlacementService();
    }

    public function dspSources()
    {
        $title = 'DSP Sources';

        return view('vue', compact('title'));
    }

    public function getDspSourcesTable(Request $request) {

        $DspSettingsPrebidSourceMapper = new DspSettingsPrebidSourceMapper();

        $orderBy = $request->has("orderBy") ? $request->orderBy : 'dspCompanyName';
        $sortedBy = $request->has("sortedBy") ? $request->sortedBy : 'DESC';
        $filters = json_decode($request->get('filters'), true);
        $filters = array_diff($filters, array('', false));
        $offset = $request->get('offset');

        $dspSources = $DspSettingsPrebidSourceMapper
            ->select('dspCompanyName', 'source', 'prebidExt');

        if (!empty($filters)) {
            $dspSources->where(function ($query) use ($filters) {
                foreach ($filters as $column => $value) {
                    $query->orWhere("$column", 'LIKE', "%{$value}%");
                }
            });
        }

        $res = $dspSources->orderBy($orderBy, $sortedBy);

        if ($offset) {
            $listDspSources = $res->offset($offset)->limit(1)->get();
        }else {
            $listDspSources = $res->paginate(50)->toArray();
        }

        return response()->json([
            'userRole' => Auth::user()->role,
            'listDsp' => $listDspSources,
        ]);
    }

    public function dspSourcesCreateVue()
    {
        $title = 'Create DSP Source';

        return view('vue', compact('title'));
    }

    public function dspSourcesCreateData(Request $request)
    {
        $listDSPCompany = new ListDSP();
        $prebidPartners = new PrebidPartners();
        $listDspCompany = $prebidPartners->select('name')->pluck('name')->toArray();
        $listPrebidPartners = $prebidPartners->get()->keyBy('name')->toArray();

        return response()->json([
            'listDspCompany' => $listDspCompany,
            'schema' => $listPrebidPartners['conversant'],
            'listPrebidPartners' => $listPrebidPartners
        ]);
    }

    public function dspSourcesCreate(Request $request) {

        $requestDTO = PrebidSourceDTO::fromRequest($request);
        $dspSettingsPrebidSourceMapper = new DspSettingsPrebidSourceMapper();

        $dspCompanyName = $requestDTO->dspCompanyName ?? '';
        $source = $requestDTO->source ?? '';
        $partner = $requestDTO->partner ?? '';
        $prebid = $requestDTO->prebid ?? [];

        if (empty($requestDTO->dspCompanyName)) {
            $errors[] = 'Company name is required';
        } elseif (empty($requestDTO->source)) {
            $errors[] = 'Source name is required';
        } elseif (empty($requestDTO->partner)) {
            $errors[] = 'Partner name is required';
        }


        $uniqueDspSource = $dspSettingsPrebidSourceMapper
            ->where('source', '=', $source)
            ->where('dspCompanyName', '=', $dspCompanyName)
            ->first();

        if ($uniqueDspSource) {
            $errors[] = 'Dsp Source already exists!';
        }

        if (empty($errors)) {

            try {
                $prebidPartnerInfo = (new PrebidPartners)->getPartnerInfo($partner);

                $inputJson = !empty($requestDTO->prebid) ? json_decode(json_encode($requestDTO->prebid)) : (object)[];
                $partnerJsonSchema = $prebidPartnerInfo->params ?? [];

                $validator = new \JsonSchema\Validator();
                $validator->validate($inputJson, $partnerJsonSchema, Constraint::CHECK_MODE_EXCEPTIONS);

                if ($validator->isValid()) {
                    $prebidExt = [];
                    $prebidExt[$partner] = $prebid;

                    $dspSettingsPrebidSourceMapper->dspCompanyName = $dspCompanyName;
                    $dspSettingsPrebidSourceMapper->source = $source;
                    $dspSettingsPrebidSourceMapper->prebidExt = json_encode($prebidExt);
                    $dspSettingsPrebidSourceMapper->save();
                }

            } catch (\Exception $e) {
                return response()->json([
                    'status' => 'error',
                    'errors' => [$e->getMessage()]
                ], 400);
            }

            return response()->json([
                'status' => 'success',
            ]);

        } else {

            return response()->json([
                'status' => 'error',
                'errors' => array_unique($errors)
            ], 400);
        }
    }

    public function dspSourcesEditVue() {
        $title = 'Edit Dsp Source';
        return view('vue', compact('title'));
    }

    public function dspSourcesSettings($company, $source)
    {

        $dspSettingsPrebidSourceMapper = new DspSettingsPrebidSourceMapper();
        $prebidPartners = new PrebidPartners();

        $dspSource = $dspSettingsPrebidSourceMapper
            ->select('dspCompanyName', 'source', 'prebidExt')
            ->where('dspCompanyName', '=', $company)
            ->where('source', '=', $source)
            ->first();


        if (is_null($dspSource)) {
            return response() ->json([
                'success' => false,
                'errors' => ['Not Found']
            ], 404);
        }

        $listDspCompany = $prebidPartners->select('name')->pluck('name')->toArray();
        $listPrebidPartners = $prebidPartners->get()->keyBy('name')->toArray();

        $prebid = $this->prepareSettingsPrebidInfo($dspSource);

        return response()->json([
            'dspSource' => $dspSource,
            'prebid' => $prebid,
            'listDspCompany' => $listDspCompany,
            'listPrebidPartners' => $listPrebidPartners
        ]);
    }


    public function dspSourcesUpdate(Request $request): object {

        $requestDTO = PrebidSourceDTO::fromRequest($request);
        $dspSettingsPrebidSourceMapper = new DspSettingsPrebidSourceMapper();
        $dspCompanyName = $requestDTO->dspCompanyName ?? '';
        $source = $requestDTO->source ?? '';

        $dspSource = $dspSettingsPrebidSourceMapper
            ->select('dspCompanyName', 'source', 'prebidExt')
            ->where('source', '=', $source)
            ->where('dspCompanyName', '=', $dspCompanyName)
            ->first();

        if ($requestDTO->prebid && !empty($requestDTO->prebid['partner'])) {
            $prebidPartner = $requestDTO->prebid['partner'];
        } else {
            $prebidInputs = $requestDTO->prebid ? (array)json_decode($dspSource->prebidExt) : [];
            $prebidPartner = array_keys($prebidInputs)[0];
        }

        $prebidInputs = $requestDTO->prebid ? $requestDTO->prebid['inputs'] : [];

        try {
            $prebidPartnerInfo = (new PrebidPartners)->getPartnerInfo($prebidPartner);
            $inputJson = !empty($prebidInputs) ? json_decode(json_encode($prebidInputs)) : (object)[];
            $partnerJsonSchema = $prebidPartnerInfo->params ?? [];

            $validator = new \JsonSchema\Validator();
            $validator->validate($inputJson, $partnerJsonSchema, Constraint::CHECK_MODE_EXCEPTIONS);

            if ($validator->isValid()) {
                $preparePrebidExt = [];
                $preparePrebidExt[$prebidPartner] = $prebidInputs;
                $prebidExtNew = json_encode($preparePrebidExt);

                $dspSettingsPrebidSourceMapper
                    ->where('source', '=', $source)
                    ->where('dspCompanyName', '=', $dspCompanyName)
                    ->update(['prebidExt' => $prebidExtNew]);
            }

        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'errors' => [$e->getMessage()]
            ], 400);
        }

        return response()->json([
            'success' => true,
            'message' => 'Dsp Source is saved!'
        ]);
    }

    public function dspSourcesDelete(Request $request) {

        $requestDTO = PrebidSourceDTO::fromRequest($request);
        $dspSettingsPrebidSourceMapper = new DspSettingsPrebidSourceMapper();

        $dspSource = $dspSettingsPrebidSourceMapper
            ->where('source', '=', $requestDTO->source)
            ->where('dspCompanyName', '=', $requestDTO->dspCompanyName)
            ->first();

        if (!$dspSource) {
            return response()->json([
                'status' => 'error',
                'errors' => ['Unknown Dsp Source']
            ], 400);
        }

        if ($dspSettingsPrebidSourceMapper->where([
            ['source', '=', $requestDTO->source],
            ['dspCompanyName', '=', $requestDTO->dspCompanyName],
        ])->delete()) {
            return response()->json([
                'status' => 'success',
            ]);
        }

        return response()->json([
            'status' => 'error',
            'errors' => ['Dsp Source delete error']
        ], 400);
    }

    private function prepareSettingsPrebidInfo($dsp): array
    {
        $prebidInputs = [];
        $prebidPartnerInfo = [];
        $prebidSupportedFormats = [];

        if (!empty($dsp->prebidExt)) {
            $prebidInputs = (array)json_decode($dsp->prebidExt);
            $prebidPartner = array_keys($prebidInputs)[0];

            $prebidInputs = $prebidInputs[$prebidPartner];
            $prebidPartnerInfo = (new PrebidPartners)->getPartnerInfo($prebidPartner);
            $prebidSupportedFormats = $prebidPartnerInfo ? explode(',', $prebidPartnerInfo->supportedFormats) : '';
        }

        return [
            'inputs' => $prebidInputs,
            'info' => $prebidPartnerInfo,
            'formats' => $prebidSupportedFormats,
        ];
    }
}
